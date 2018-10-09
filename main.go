package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"hash"
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

type App struct {
	splitSize       uint
	concurrent      uint
	errorCapacity   uint
	requestRange    string
	requestInterval time.Duration
	referer         string
	userAgent       string
	primaryURL      string
}

func main() {
	var app App
	os.Exit(app.Main())
}

func (app *App) Main() int {
	var showStatus bool
	flag.UintVar(&app.splitSize, "s", _defaultSplitSize, "split size (MiB)")
	flag.UintVar(&app.concurrent, "c", _defaultConcurrent, "maximum number of parallel downloads")
	flag.UintVar(&app.errorCapacity, "e", _defaultErrorCapacity, "maximum number of errors")
	flag.StringVar(&app.requestRange, "r", "", "request range (MiB), e.g., 0-1023")
	flag.DurationVar(&app.requestInterval, "i", _defaultRequestInterval, "request interval")
	flag.BoolVar(&showStatus, "status", false, "show status, then exit")
	flag.Parse()

	var err error
	var client = http.DefaultClient

	if !showStatus {
		app.Print("loading Referer...")
		app.referer, err = _loadURL(filepath.Join(".", "Referer"))

		if err == nil || os.IsNotExist(err) {
			app.Print("\033[1K\r")
			app.Print("loading UserAgent...")
			app.userAgent, err = _loadSingleLine(filepath.Join(".", "UserAgent"), 1024)
		}

		if err == nil || os.IsNotExist(err) {
			var jar http.CookieJar
			app.Print("\033[1K\r")
			app.Print("loading Cookies...")
			jar, err = _loadCookies(filepath.Join(".", "Cookies"))
			if err == nil {
				client = &http.Client{Jar: jar}
			}
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
		app.Println(err)
		return 1
	}

	app.Print("\033[1K\r")
	app.Print("opening File...")
	file, err := _openDataFile(fileName)
	if err != nil {
		app.Println(err)
		return 1
	}
	defer func() {
		err := file.Close()
		if err != nil {
			app.Println(err)
		}
	}()

	if fileSize > 0 {
		app.Print("\033[1K\r")
		app.Print("loading Hash...")
		err := file.LoadHashFile()
		if err != nil && !os.IsNotExist(err) {
			app.Println(err)
			return 1
		}
	}

	app.Print("\033[1K\r")

	if showStatus {
		app.status(file)
		return 2
	}

	if flag.NArg() > 0 {
		app.Print("parsing URL...")
		u, err := url.Parse(flag.Arg(0))
		if err != nil {
			app.Println(err)
			return 1
		}
		app.primaryURL = u.String()
	} else {
		app.Print("loading URL...")
		url, err := _loadURL(filepath.Join(".", "URL"))
		if err != nil {
			app.Println(err)
			return 1
		}
		app.primaryURL = url
	}

	app.Print("\033[1K\r")

	if app.requestRange != "" {
		var requestRange RangeSet
		for _, r := range strings.Split(app.requestRange, ",") {
			r := strings.Split(r, "-")
			if len(r) > 2 {
				app.Println("request range is invalid")
				return 1
			}
			i, err := strconv.ParseInt(r[0], 10, 32)
			if err != nil {
				app.Println("request range is invalid")
				return 1
			}
			if len(r) == 1 {
				requestRange.AddRange(i*1024*1024, (i+1)*1024*1024)
				continue
			}
			if r[1] == "" {
				requestRange.AddRange(i*1024*1024, math.MaxInt64)
				continue
			}
			j, err := strconv.ParseInt(r[1], 10, 32)
			if err != nil || j < i {
				app.Println("request range is invalid")
				return 1
			}
			requestRange.AddRange(i*1024*1024, (j+1)*1024*1024)
		}
		if len(requestRange) > 0 {
			file.SetRange(requestRange)
		}
	}

	for i := 0; i < 2; i++ {
		fileSize := file.FileSize()
		completeSize := file.CompleteSize()
		if fileSize == 0 || completeSize != fileSize {
			app.dl(file, client)
			fileSize = file.FileSize()
			completeSize = file.CompleteSize()
			if fileSize == 0 || completeSize != fileSize {
				return 1
			}
		}

		var digest hash.Hash

		fileMD5 := strings.ToLower(file.FileMD5())
		if len(fileMD5) == 32 {
			digest = md5.New()
			app.Println("Content-MD5:", fileMD5)
		}

		shouldVerify := i == 0 || digest != nil
		if !shouldVerify {
			return 0
		}

		app.Print("verifying...")
		if err := file.Verify(digest); err != nil {
			app.Println(err)
			return 1
		}

		if file.CompleteSize() != fileSize {
			app.Println("BAD")
			continue
		}

		if digest != nil && hex.EncodeToString(digest.Sum(nil)) != fileMD5 {
			app.Println("BAD")
			return 1
		}

		app.Println("OK")
		return 0
	}

	return 1
}

func (app *App) dl(file *DataFile, client *http.Client) {
	type (
		PrintMessage struct{}

		ResponseMessage struct{}

		ProgressMessage struct {
			Just int
		}

		CompleteMessage struct {
			Err       error
			Fatal     bool
			Responsed bool
		}

		StatInfo struct {
			Time time.Time
			Size int64
		}
	)

	const (
		statCapacity = 30
		instantCount = 5
	)

	var (
		statList      = make([]StatInfo, 0, statCapacity)
		statCurrent   = StatInfo{time.Now(), 0}
		firstRecvTime time.Time
		totalReceived int64
		responseCount int
	)

	topCtx := context.TODO()

	queuedMessages := observable.NewSubject()
	queuedMessagesCtx, _ := queuedMessages.Congest(int(app.concurrent*3)).Subscribe(
		topCtx,
		func(t observable.Notification) {
			shouldPrint := false
			switch {
			case t.HasValue:
				switch v := t.Value.(type) {
				case ProgressMessage:
					statCurrent.Size += int64(v.Just)
					if totalReceived == 0 {
						firstRecvTime = time.Now()
						log.Println("first arrival")
					}
					totalReceived += int64(v.Just)

				case PrintMessage:
					if len(statList) == statCapacity {
						copy(statList, statList[1:])
						statList = statList[:statCapacity-1]
					}
					statList = append(statList, statCurrent)
					statCurrent = StatInfo{time.Now(), 0}
					shouldPrint = totalReceived > 0

				case ResponseMessage:
					responseCount++

				case CompleteMessage:
					if v.Responsed {
						responseCount--
					}
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
						app.Print("\033[1K\r")
						log.Println(unwrappedErr)
						shouldPrint = totalReceived > 0
					}
				}
			default:
				shouldPrint = totalReceived > 0
			}
			if shouldPrint {
				app.Print("\033[1K\r")

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
					app.Printf("%v%% [%v] ", progress, s)
				}

				app.Printf("CN:%v", responseCount)

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
					app.Printf(" DL:%vB/s", _formatBytes(int64(speed)))
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
						seconds := int64(math.Ceil(remaining / speed))
						app.Printf(" ETA:%v", _formatDuration(time.Duration(seconds)*time.Second))
						if seconds < int64(len(statList)) {
							// about to complete, keep only most recent stats
							statList = append(statList[:0], statList[len(statList)-int(seconds):]...)
						}
					}
				}

				if !t.HasValue {
					// about to exit, keep this status line
					app.Println()
				}
			}
			if !t.HasValue && totalReceived > 0 {
				timeUsed := time.Since(firstRecvTime)
				log.Printf(
					"recv %vB in %v, %vB/s\n",
					_formatBytes(totalReceived),
					timeUsed.Round(time.Second),
					_formatBytes(int64(float64(totalReceived)/timeUsed.Seconds())),
				)
			}
		},
	)
	defer func() {
		queuedMessages.Complete()
		<-queuedMessagesCtx.Done()
	}()

	{
		ctx, cancel := observable.Interval(time.Second).
			MapTo(PrintMessage{}).Subscribe(topCtx, queuedMessages.Observer)
		defer func() {
			cancel()
			<-ctx.Done()
		}()
	}

	queuedWrites := observable.NewSubject()
	queuedWritesCtx, _ := queuedWrites.Congest(int(app.concurrent*3)).Subscribe(
		topCtx,
		func(t observable.Notification) {
			if t.HasValue {
				t.Value.(func())()
			}
		},
	)
	defer func() {
		queuedWrites.Complete()
		<-queuedWritesCtx.Done()
		file.Sync()
	}()

	bufferPool := sync.Pool{
		New: func() interface{} {
			buf := make([]byte, _readBufferSize)
			return &buf
		},
	}

	readAndWrite := func(body io.Reader, offset int64) (n int, err error) {
		buf := *bufferPool.Get().(*[]byte)
		n, err = body.Read(buf)
		if n > 0 {
			queuedWrites.Next(func() {
				file.WriteAt(buf[:n], offset)
				bufferPool.Put(&buf)
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
		splitSize := int64(app.splitSize) * 1024 * 1024
		if file.FileSize() > 0 {
			size := (file.FileSize() - file.CompleteSize()) / int64(app.concurrent)
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
	activeTasksCtx, _ := activeTasks.MergeAll().Subscribe(topCtx, queuedMessages.Observer)
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
		activeCount  uint
		beingPaused  bool
		shouldDelay  bool
		beingDelayed <-chan time.Time
		errorCount   uint
		fatalErrors  bool
		currentURL   = app.primaryURL
		onMessage    = make(chan interface{}, app.concurrent)
	)

	for {
		switch {
		case activeCount >= app.concurrent:
		case beingPaused:
		case shouldDelay:
			shouldDelay = false
			beingDelayed = time.After(app.requestInterval)
		case beingDelayed != nil:
		case fatalErrors:
			if activeCount == 0 {
				return
			}
		case errorCount >= app.errorCapacity:
			if activeCount == 0 {
				return
			}
			if currentURL == app.primaryURL {
				break
			}
			currentURL = app.primaryURL
			errorCount = 0
			fallthrough
		default:
			offset, size := takeIncomplete()
			if size == 0 {
				if activeCount == 0 {
					return
				}
				break
			}

			cr := func(parent context.Context, ob observable.Observer) (ctx context.Context, cancel context.CancelFunc) {
				ctx, cancel = context.WithCancel(activeCtx)

				var (
					err   error
					fatal bool
				)
				defer func() {
					if err != nil {
						returnIncomplete(offset, size)
						ob.Next(CompleteMessage{err, fatal, false})
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

				if app.referer != "" {
					req.Header.Set("Referer", app.referer)
				}
				if app.userAgent != "" {
					req.Header.Set("User-Agent", app.userAgent)
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

				ob.Next(ResponseMessage{})

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
						ob.Next(CompleteMessage{err, false, true})
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
							ob.Next(ProgressMessage{n})
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

			do := func(t observable.Notification) {
				if t.HasValue {
					switch t.Value.(type) {
					case ProgressMessage:

					case ResponseMessage:
						onMessage <- t.Value

					case CompleteMessage:
						onMessage <- t.Value
					}
				}
			}

			activeCount++
			beingPaused = true
			shouldDelay = true

			activeTasks.Next(observable.Create(cr).Do(do))
		}

		select {
		case <-waitSignal:
			return
		case <-beingDelayed:
			beingDelayed = nil
		case e := <-onMessage:
			switch e := e.(type) {
			case ResponseMessage:
				beingPaused = false
				errorCount = 0
			case CompleteMessage:
				activeCount--
				if !e.Responsed {
					beingPaused = false
					if e.Err != nil {
						errorCount++
					} else {
						shouldDelay = false
					}
				}
				if e.Err != nil && e.Fatal {
					fatalErrors = true
				}
			}
		}
	}
}

func (app *App) status(file *DataFile) {
	var (
		fileSize     = file.FileSize()
		completeSize = file.CompleteSize()
		fileMD5      = file.FileMD5()
	)
	if fileSize > 0 {
		progress := int(float64(completeSize) / float64(fileSize) * 100)
		fmt.Println("Size:", fileSize)
		fmt.Println("Complete:", completeSize, fmt.Sprintf("(%v%%)", progress))
	} else {
		fmt.Println("Complete:", completeSize)
	}
	if fileMD5 != "" {
		fmt.Println("Content-MD5:", fileMD5)
	}
	var items []string
	for _, r := range file.Incomplete() {
		i := int(r.Low / (1024 * 1024))
		if r.High == math.MaxInt64 {
			items = append(items, fmt.Sprintf("%v-", i))
			break
		}
		j := int(r.High / (1024 * 1024))
		if i+1 == j {
			items = append(items, fmt.Sprint(i))
			continue
		}
		items = append(items, fmt.Sprintf("%v-%v", i, j-1))
	}
	if len(items) > 0 {
		fmt.Println("Incomplete(MiB):", strings.Join(items, ","))
	}
}

func (*App) Print(a ...interface{}) {
	fmt.Fprint(os.Stderr, a...)
}

func (*App) Printf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func (*App) Println(a ...interface{}) {
	fmt.Fprintln(os.Stderr, a...)
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

func _formatDuration(d time.Duration) (s string) {
	switch {
	case d < time.Minute:
		s = d.Round(time.Second).String()
	case d < time.Hour:
		s = d.Round(time.Minute).String()
	default:
		s = d.Round(time.Hour).String()
	}
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return
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

		if line == "" || strings.HasPrefix(line, "# ") {
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

		if strings.HasPrefix(cookie.Domain, "#HttpOnly_") {
			cookie.Domain = cookie.Domain[10:]
			cookie.HttpOnly = true
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
