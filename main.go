package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/b97tsk/observable"
	"golang.org/x/net/publicsuffix"
)

const (
	_defaultSplitSize       = 80 // MiB
	_defaultConcurrent      = 4
	_defaultErrorCapacity   = 4
	_defaultRequestInterval = 2 * time.Second
	_readBufferSize         = 4096
	_readTimeout            = 30 * time.Second
)

type (
	_PrintMessage struct{}

	_ResponseMessage struct{}

	_ProgressMessage struct {
		Just int
	}

	_CompleteMessage struct {
		Err   error
		Fatal bool
	}
)

func main() {
	var (
		splitSize       uint
		concurrent      uint
		errorCapacity   uint
		requestInterval time.Duration
	)
	flag.UintVar(&splitSize, "s", _defaultSplitSize, "split size (MiB)")
	flag.UintVar(&concurrent, "c", _defaultConcurrent, "maximum number of parallel downloads")
	flag.UintVar(&errorCapacity, "e", _defaultErrorCapacity, "maximum number of errors")
	flag.DurationVar(&requestInterval, "i", _defaultRequestInterval, "request interval")
	flag.Parse()

	fmt.Print("\033[1K\r")
	fmt.Print("loading URL...")
	primaryURL, err := _loadURL(filepath.Join(".", "URL"))
	if err != nil {
		fmt.Println(err)
		return
	}

	var referer string
	if err == nil || os.IsNotExist(err) {
		fmt.Print("\033[1K\r")
		fmt.Print("loading Referer...")
		referer, err = _loadURL(filepath.Join(".", "Referer"))
	}

	var userAgent string
	if err == nil || os.IsNotExist(err) {
		fmt.Print("\033[1K\r")
		fmt.Print("loading UserAgent...")
		userAgent, err = _loadSingleLine(filepath.Join(".", "UserAgent"), 1024)
	}

	client := http.DefaultClient
	if err == nil || os.IsNotExist(err) {
		var jar http.CookieJar
		fmt.Print("\033[1K\r")
		fmt.Print("loading Cookies...")
		jar, err = _loadCookies(filepath.Join(".", "Cookies"))
		if err == nil {
			client = &http.Client{Jar: jar}
		}
	}

	fileName := filepath.Join(".", "File")
	fileSize := int64(0)
	if err == nil || os.IsNotExist(err) {
		var info os.FileInfo
		info, err = os.Stat(fileName)
		if err == nil {
			fileSize = info.Size()
		}
	}

	if err != nil && !os.IsNotExist(err) {
		fmt.Println(err)
		return
	}

	fmt.Print("\033[1K\r")
	fmt.Print("opening File...")
	file, err := _openDataFile(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	if fileSize > 0 {
		fmt.Print("\033[1K\r")
		fmt.Print("loading Hash...")
		err := file.LoadHashFile()
		if err != nil && !os.IsNotExist(err) {
			fmt.Println(err)
			return
		}
	}

	fmt.Print("\033[1K\r")

	defer func() {
		fileSize := file.FileSize()
		if fileSize == 0 {
			return
		}
		completeSize := file.CompleteSize()
		if completeSize != fileSize {
			return
		}
		fileMD5 := strings.ToLower(file.FileMD5())
		if len(fileMD5) != 32 {
			return
		}
		fmt.Print("\033[1K\r")
		fmt.Println("Content-MD5:", fileMD5)
		fmt.Print("verifying...")
		digest := md5.New()
		if err := file.Checksum(digest); err != nil {
			fmt.Println(err)
			return
		}
		if hex.EncodeToString(digest.Sum(nil)) != fileMD5 {
			fmt.Println("BAD")
			return
		}
		fmt.Println("OK")
	}()

	type stat struct {
		Time time.Time
		Size int64
	}

	const (
		statCapacity = 30
		instantCount = 5
	)

	var (
		statList      = make([]stat, 0, statCapacity)
		statCurrent   = stat{time.Now(), 0}
		firstRecvTime time.Time
		totalReceived int64
		activeCount   uint32
	)

	topCtx := context.TODO()

	queuedMessages := observable.NewSubject()
	queuedMessagesCtx, _ := queuedMessages.Congest(int(concurrent*2)).Subscribe(
		topCtx,
		observable.ObserverFunc(
			func(t observable.Notification) {
				shouldPrint := false
				switch {
				case t.HasValue:
					switch v := t.Value.(type) {
					case _ProgressMessage:
						statCurrent.Size += int64(v.Just)
						if totalReceived == 0 {
							firstRecvTime = time.Now()
						}
						totalReceived += int64(v.Just)

					case _PrintMessage:
						if len(statList) == statCapacity {
							copy(statList, statList[1:])
							statList = statList[:statCapacity-1]
						}
						statList = append(statList, statCurrent)
						statCurrent = stat{time.Now(), 0}
						shouldPrint = true

					case _CompleteMessage:
						unwrappedErr := v.Err
						switch e := unwrappedErr.(type) {
						case *net.OpError:
							unwrappedErr = e.Err
						case *url.Error:
							unwrappedErr = e.Err
						}
						switch unwrappedErr {
						case nil, io.EOF, io.ErrUnexpectedEOF, context.Canceled:
						default:
							fmt.Print("\033[1K\r")
							log.Println(unwrappedErr)
							shouldPrint = true
						}
					}
				default:
					shouldPrint = true
				}
				if shouldPrint {
					fmt.Print("\033[1K\r")

					fileSize := file.FileSize()
					completeSize := file.CompleteSize()

					{
						const length = 20
						progress := 0
						if fileSize > 0 {
							progress = int(float64(completeSize) / float64(fileSize) * 100)
						}
						s := strings.Repeat("=", length*progress/100)
						if len(s) < length && completeSize > 0 {
							s += ">"
						}
						if len(s) < length {
							s += strings.Repeat("-", length-len(s))
						}
						fmt.Printf("%v%% [%v] ", progress, s)
					}

					fmt.Printf("DL:%v", atomic.LoadUint32(&activeCount))

					{
						stat := statCurrent
						statList := statList
						if len(statList) >= instantCount {
							statList = statList[len(statList)-instantCount:]
						}
						if len(statList) > 0 {
							stat.Time = statList[0].Time
						}
						for _, s := range statList {
							stat.Size += s.Size
						}
						speed := float64(stat.Size) / time.Since(stat.Time).Seconds()
						fmt.Printf(" %vB/s", _formatBytes(int64(speed)))
					}

					if fileSize > 0 {
						stat := statCurrent
						if len(statList) > 0 {
							stat.Time = statList[0].Time
						}
						for _, s := range statList {
							stat.Size += s.Size
						}
						if stat.Size > 0 {
							speed := float64(stat.Size) / time.Since(stat.Time).Seconds()
							remaining := float64(fileSize - completeSize)
							fmt.Printf(" %v", time.Duration(remaining/speed)*time.Second)
						}
					}

					if !t.HasValue {
						// about to exit, keep this status line
						fmt.Println()
					}
				}
				if !t.HasValue && totalReceived > 0 {
					timeUsed := time.Since(firstRecvTime)
					log.Printf(
						"recv %vB in %v, %vB/s\n",
						_formatBytes(totalReceived),
						timeUsed.Truncate(time.Second),
						_formatBytes(int64(float64(totalReceived)/timeUsed.Seconds())),
					)
				}
			},
		),
	)
	defer func() {
		queuedMessages.Complete()
		<-queuedMessagesCtx.Done()
	}()

	{
		ctx, cancel := observable.Interval(time.Second).
			MapTo(_PrintMessage{}).Subscribe(topCtx, queuedMessages)
		defer func() {
			cancel()
			<-ctx.Done()
		}()
	}

	queuedWrites := observable.NewSubject()
	queuedWritesCtx, _ := queuedWrites.Congest(int(concurrent*2)).Subscribe(
		topCtx,
		observable.ObserverFunc(
			func(t observable.Notification) {
				if t.HasValue {
					t.Value.(func())()
				}
			},
		),
	)
	defer func() {
		queuedWrites.Complete()
		<-queuedWritesCtx.Done()
		file.Sync()
	}()

	bufferPool := sync.Pool{
		New: func() interface{} { return make([]byte, _readBufferSize) },
	}

	readAndWrite := func(body io.Reader, offset int64) (n int, err error) {
		buf := bufferPool.Get().([]byte)
		n, err = body.Read(buf)
		if n > 0 {
			queuedWrites.Next(func() {
				file.WriteAt(buf[:n], offset)
				bufferPool.Put(buf)
			})
		}
		return
	}

	waitWriteDone := func() {
		q := make(chan struct{})
		queuedWrites.Next(func() { close(q) })
		<-q
	}

	takeIncomplete := func() (offset, size int64) {
		splitSize := int64(splitSize) * 1024 * 1024
		if file.FileSize() > 0 {
			size := (file.FileSize() - file.CompleteSize()) / int64(concurrent)
			if size < splitSize {
				splitSize = size
			}
		}
		return file.TakeIncomplete(splitSize)
	}

	returnIncomplete := func(offset, size int64) {
		file.ReturnIncomplete(offset, size)
	}

	activeTasks := observable.NewSubject()
	activeTasksCtx, _ := activeTasks.MergeAll().Subscribe(topCtx, queuedMessages)
	defer func() {
		activeTasks.Complete()
		<-activeTasksCtx.Done()
	}()

	activeCtx, activeCancel := context.WithCancel(topCtx)
	defer activeCancel()

	waitSignal := make(chan os.Signal, 1)
	signal.Notify(waitSignal, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(waitSignal)

	var (
		beingPaused    uint32
		shouldDelay    uint32
		beingDelayed   <-chan time.Time
		errorCount     uint32
		hasFatalErrors uint32
		stateChanged   = make(chan struct{}, 1)
		currentURL     = primaryURL
	)

	for {
		switch {
		case atomic.LoadUint32(&activeCount) >= uint32(concurrent):
		case atomic.LoadUint32(&beingPaused) != 0:
		case atomic.LoadUint32(&shouldDelay) != 0:
			atomic.StoreUint32(&shouldDelay, 0)
			beingDelayed = time.After(requestInterval)
		case beingDelayed != nil:
		case atomic.LoadUint32(&hasFatalErrors) != 0:
		case atomic.LoadUint32(&errorCount) >= uint32(errorCapacity):
			if atomic.LoadUint32(&activeCount) == 0 {
				return
			}
			if currentURL == primaryURL {
				break
			}
			currentURL = primaryURL
			errorCount = 0
			fallthrough
		default:
			offset, size := takeIncomplete()
			if size == 0 {
				if atomic.LoadUint32(&activeCount) == 0 {
					return
				}
				break
			}

			atomic.AddUint32(&activeCount, 1)
			atomic.StoreUint32(&beingPaused, 1)
			atomic.StoreUint32(&shouldDelay, 1)

			cr := func(parent context.Context, ob observable.Observer) (ctx context.Context, cancel context.CancelFunc) {
				ctx, cancel = context.WithCancel(activeCtx)

				var (
					err   error
					fatal bool
				)
				defer func() {
					if err != nil {
						returnIncomplete(offset, size)
						ob.Next(_CompleteMessage{err, fatal})
						ob.Complete()
						cancel()
					}
				}()

				req, err := http.NewRequest(http.MethodGet, currentURL, nil)
				if err != nil {
					return
				}

				shouldLimitSize := false
				if file.FileSize() > 0 {
					req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+size-1))
				} else {
					req.Header.Set("Range", fmt.Sprintf("bytes=%v-", offset))
					shouldLimitSize = true
				}

				if referer != "" {
					req.Header.Set("Referer", referer)
				}
				if userAgent != "" {
					req.Header.Set("User-Agent", userAgent)
				}

				req = req.WithContext(ctx)

				resp, err := client.Do(req)
				if err != nil {
					return
				}

				defer func() {
					if err != nil {
						resp.Body.Close()
					}
				}()

				if resp.StatusCode == 200 {
					err = errors.New("this server does not support partial requests")
					fatal = true
					return
				}

				if resp.StatusCode != 206 {
					err = errors.New(resp.Status)
					return
				}

				contentRange := resp.Header.Get("Content-Range")
				if contentRange == "" {
					err = errors.New("Content-Range not found")
					fatal = true
					return
				}

				re := regexp.MustCompile(`^bytes (\d+)-(\d+)/(\d+)$`)
				slice := re.FindStringSubmatch(contentRange)
				if slice == nil {
					err = fmt.Errorf("Content-Range unrecognized: %v", contentRange)
					fatal = true
					return
				}

				shouldSync := false

				contentLength, _ := strconv.ParseInt(slice[3], 10, 64)
				switch file.FileSize() {
				case contentLength:
				case 0:
					shouldSync = true
					file.SetFileSize(contentLength)
				default:
					err = errors.New("Content-Length mismatched")
					fatal = true
					return
				}

				contentMD5 := resp.Header.Get("Content-MD5")
				switch file.FileMD5() {
				case contentMD5:
				case "":
					shouldSync = true
					file.SetFileMD5(contentMD5)
				default:
					err = errors.New("Content-MD5 mismatched")
					fatal = true
					return
				}

				eTag := resp.Header.Get("ETag")
				switch file.EntityTag() {
				case eTag:
				case "":
					shouldSync = true
					file.SetEntityTag(eTag)
				default:
					err = errors.New("ETag mismatched")
					fatal = true
					return
				}

				if eTag == "" {
					lastModified := resp.Header.Get("Last-Modified")
					switch file.LastModified() {
					case lastModified:
					case "":
						shouldSync = true
						file.SetLastModified(lastModified)
					default:
						err = errors.New("Last-Modified mismatched")
						fatal = true
						return
					}
				}

				if shouldSync {
					file.SyncNow()
				}

				if req != resp.Request {
					currentURL = resp.Request.URL.String()
				}

				body := io.Reader(resp.Body)
				if shouldLimitSize {
					returnIncomplete(offset, size)
					previousOffset := offset
					offset, size = takeIncomplete()
					if offset != previousOffset {
						panic("offset != previousOffset")
					}
					body = io.LimitReader(body, size)
				}

				ob.Next(_ResponseMessage{})

				go func() (err error) {
					var (
						readTimer        = time.AfterFunc(_readTimeout, cancel)
						shouldResetTimer bool
						shouldWaitWrite  bool
					)

					defer func() {
						resp.Body.Close()
						if shouldWaitWrite {
							waitWriteDone()
						}
						returnIncomplete(offset, size)
						ob.Next(_CompleteMessage{err, false})
						ob.Complete()
						cancel()
					}()

					for {
						if shouldResetTimer {
							readTimer.Reset(_readTimeout)
						}
						n, err := readAndWrite(body, offset)
						readTimer.Stop()
						shouldResetTimer = true
						if n > 0 {
							offset, size = offset+int64(n), size-int64(n)
							shouldWaitWrite = true
							ob.Next(_ProgressMessage{n})
						}
						if err != nil {
							if err == io.EOF {
								return nil
							}
							return err
						}
					}
				}()

				return
			}

			hasResponseMessage := false

			do := func(t observable.Notification) {
				if t.HasValue {
					switch v := t.Value.(type) {
					case _ProgressMessage:

					case _ResponseMessage:
						hasResponseMessage = true
						atomic.StoreUint32(&beingPaused, 0)
						atomic.StoreUint32(&errorCount, 0)

						select {
						case stateChanged <- struct{}{}:
						default:
						}

					case _CompleteMessage:
						atomic.AddUint32(&activeCount, math.MaxUint32)
						if !hasResponseMessage {
							atomic.StoreUint32(&beingPaused, 0)
							if v.Err != nil {
								atomic.AddUint32(&errorCount, 1)
							} else {
								atomic.StoreUint32(&shouldDelay, 0)
							}
						}
						if v.Err != nil && v.Fatal {
							atomic.StoreUint32(&hasFatalErrors, 1)
						}

						select {
						case stateChanged <- struct{}{}:
						default:
						}
					}
				}
			}

			activeTasks.Next(observable.Create(cr).Do(observable.ObserverFunc(do)))
		}

		select {
		case <-waitSignal:
			return
		case <-stateChanged:
		case <-beingDelayed:
			beingDelayed = nil
		}
	}
}

func _formatBytes(n int64) string {
	if n < 1024 {
		return strconv.FormatInt(n, 10)
	}
	kilobytes := int64(math.Ceil(float64(n) / 1024))
	if kilobytes < 1000 {
		return strconv.FormatInt(kilobytes, 10) + "K"
	}
	if kilobytes < 102400 {
		return fmt.Sprintf("%.1fM", float64(n)/(1024*1024))
	}
	megabytes := int64(math.Ceil(float64(n) / (1024 * 1024)))
	return strconv.FormatInt(megabytes, 10) + "M"
}

func _loadSingleLine(name string, max int) (line string, err error) {
	file, err := os.Open(name)
	if err != nil {
		return
	}
	defer file.Close()
	s := bufio.NewScanner(file)
	s.Buffer(nil, max)
	if s.Scan() {
		line = strings.TrimSpace(s.Text())
	}
	err = s.Err()
	return
}

func _loadURL(name string) (rawurl string, err error) {
	rawurl, err = _loadSingleLine(name, 2048)
	if err != nil {
		return
	}
	_, err = url.Parse(rawurl)
	return
}

func _loadCookies(name string) (jar http.CookieJar, err error) {
	file, err := os.Open(name)
	if err != nil {
		return
	}
	defer file.Close()

	s := bufio.NewScanner(file)
	for s.Scan() {
		line := s.Text()

		if line == "" || line[0] == '#' {
			continue
		}

		fields := strings.Split(line, "\t")
		if len(fields) != 7 {
			continue
		}

		expires, err := strconv.ParseInt(fields[4], 10, 64)
		if err != nil {
			continue
		}

		cookie := http.Cookie{
			Name:    fields[5],
			Value:   fields[6],
			Path:    fields[2],
			Domain:  fields[0],
			Expires: time.Unix(expires, 0),
			Secure:  fields[3] == "TRUE",
		}

		scheme := "http"
		host := cookie.Domain
		if cookie.Secure {
			scheme = "https"
		}
		if host != "" && host[0] == '.' {
			host = host[1:]
		}

		u, err := url.Parse(scheme + "://" + host + "/")
		if err != nil {
			continue
		}

		if jar == nil {
			jar, err = cookiejar.New(&cookiejar.Options{
				PublicSuffixList: publicsuffix.List,
			})
			if err != nil {
				return nil, err
			}
		}
		jar.SetCookies(u, []*http.Cookie{&cookie})
	}
	return
}
