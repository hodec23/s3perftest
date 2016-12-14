package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"operations"
	"runtime"
	"sync"
	"time"
	"utils"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())

	var (
		concurrency int
		numObjects  int
		bucketName  string
		operation   string
		delimiter   string
		maxKeys     int64
		prefix      string
		duration    time.Duration
	)
	// Collect options
	flag.IntVar(&concurrency, "concurrency", 5, "number of threads for operations")
	flag.StringVar(&operation, "operation", "", "createobjects/listobjects")
	defaultDuration, _ := time.ParseDuration("1h")
	flag.DurationVar(&duration, "timeout", defaultDuration, "timeout")
	flag.IntVar(&numObjects, "objnumber", 0, "number of objects to be created")
	flag.StringVar(&bucketName, "bucketname", "ecstesthugebucket", "bucket name")
	flag.StringVar(&delimiter, "delimiter", "", "delimiter")
	flag.Int64Var(&maxKeys, "maxkeys", 1000, "maxkeys")
	flag.StringVar(&prefix, "prefix", "", "prefix")
	flag.Parse()

	if operation != "createobjects" && operation != "listobjects" {
		fmt.Printf("Unsupported operation %s\n", operation)
		return
	}

	// Load config.yaml
	config := utils.LoadConfig()

	// Get S3 client to server
	s3client, err := utils.GetS3Client(config)
	utils.Check(err)

	// Get S3BucketOperations handler
	s3BucketOper := &operations.S3BucketOperations{
		S3Client:   s3client,
		BucketName: bucketName,
	}

	// Create Bucket
	err = s3BucketOper.CreateBucket()
	utils.Check(err)

	// Create work queue
	workqueue := make(chan operations.Task, 1000)

	expireTime := time.Now().Add(duration)
	// Start handlers
	log.Printf("Starting %d goroutine(s)", concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer log.Println("## goroutine exists")
			process(workqueue, s3BucketOper, expireTime)
		}()
	}

	// Create stats queue
	statsqueue := make(chan int, 100)
	errorqueue := make(chan int, 100)
	var rqWg sync.WaitGroup
	rqWg.Add(1)
	go func() {
		defer rqWg.Done()
		var (
			lastcount int
			count     int
			errCount  int
		)
		tickchan := time.Tick(time.Second)
		startedAt := time.Now()
		for {
			select {
			case r, ok := <-statsqueue:
				if !ok {
					log.Printf("Processed %d, Error %d", count, errCount)
					return
				}
				count += r
			case r, ok := <-errorqueue:
				if !ok {
					log.Printf("Processed %d, Error %d", count, errCount)
					return
				}
				errCount += r

			case <-tickchan:
				log.Printf("Processed %d, Speed %d/s (%d in last sec), Error %d",
					count, int64(count)/int64(time.Since(startedAt)/time.Second), count-lastcount, errCount)
				lastcount = count
			}
		}
	}()

	switch operation {
	case "createobjects":
		log.Printf("Start to creating %d objects ...", numObjects)
		// Create objects
		for i := 0; i < numObjects; i++ {
			workqueue <- &operations.CreateObjectTask{
				CoreTask: operations.CoreTask{
					ExpireTime: expireTime,
					StatsChan:  statsqueue,
				},
				Key: utils.GenS3NamespaceKey(20, "test", 4096, 4096),
			}
		}
		log.Println("Done")
	case "listobjects":
		markers := utils.GenNSortedS3NamespaceKeysWithPrefix(prefix, concurrency-1, 20, "test", 4096, 4096)
		log.Println("markers: ", markers)
		startMarker := ""
		for i := 0; i < len(markers); i++ {
			workqueue <- &operations.ListObjectsTask{
				CoreTask: operations.CoreTask{
					ExpireTime: expireTime,
					StatsChan:  statsqueue,
				},
				Prefix:    prefix,
				Delimiter: delimiter,
				Marker:    startMarker,
				MaxKeys:   maxKeys,
				EndMarker: markers[i],
			}
			startMarker = markers[i]
		}
		// last one, no end marker
		workqueue <- &operations.ListObjectsTask{
			CoreTask: operations.CoreTask{
				ExpireTime: expireTime,
				StatsChan:  statsqueue,
			},
			Prefix:    prefix,
			Delimiter: delimiter,
			Marker:    startMarker,
			MaxKeys:   maxKeys,
		}
	default:
		// shouldn't happen because it's checked before
		fmt.Printf("Unsupported operation %s\n", operation)
	}

	close(workqueue)
	log.Println("Waiting for shutting down goroutine(s)")
	wg.Wait()
	close(statsqueue)
	rqWg.Wait()
}

// process is the handler function for Task in each goroutine
func process(c <-chan operations.Task, s *operations.S3BucketOperations, t time.Time) {
	for task := range c {
		if task.Run(s) != nil {
			log.Println("task error")
			continue
		}
		if time.Now().After(t) {
			log.Println("task timeout")
			return
		}
	}
}
