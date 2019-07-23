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

	"github.com/b97tsk/rxgo/observable"
	"golang.org/x/net/publicsuffix"
	"gopkg.in/yaml.v2"
)

const (
	readBufferSize = 4096
	readTimeout    = 30 * time.Second
	reportInterval = 10 * time.Minute
	syncInterval   = 5 * time.Minute
)

type App struct {
	Configure
	streamToStdout bool
}

type Configure struct {
	URL               string        `yaml:"url"`
	OutFile           string        `yaml:"output"`
	SplitSize         uint          `yaml:"split-size"`
	MaxConnections    uint          `yaml:"connections"`
	MaxErrors         uint          `yaml:"errors"`
	RequestInterval   time.Duration `yaml:"request-interval"`
	RequestRange      string        `yaml:"request-range"`
	CookieFile        string        `yaml:"cookie"`
	Referer           string        `yaml:"referer"`
	UserAgents        []string      `yaml:"user-agents"`
	PerUserAgentLimit uint          `yaml:"per-user-agent-limit"`
	StreamRate        uint          `yaml:"stream-rate"`
	ETagUnreliable    bool          `yaml:"etag-unreliable"`
}

func main() {
	var app App
	os.Exit(app.Main())
}

func (app *App) Main() int {
	var (
		workdir       string
		conffile      string
		showStatus    bool
		showConfigure bool
	)
	const defaultOutFile = "File"
	flag.StringVar(&workdir, "w", ".", "working directory")
	flag.StringVar(&conffile, "f", "Configure", "configure file")
	flag.StringVar(&app.OutFile, "o", defaultOutFile, "output file")
	flag.StringVar(&app.CookieFile, "cookie", "", "cookie file")
	flag.UintVar(&app.SplitSize, "s", 0, "split size (MiB), 0 means use maximum possible")
	flag.UintVar(&app.MaxConnections, "c", 4, "maximum number of parallel downloads")
	flag.UintVar(&app.MaxErrors, "e", 3, "maximum number of errors")
	flag.DurationVar(&app.RequestInterval, "interval", 2*time.Second, "request interval")
	flag.StringVar(&app.RequestRange, "range", "", "request range (MiB), e.g., 0-1023")
	flag.BoolVar(&showStatus, "status", false, "show status, then exit")
	flag.BoolVar(&showConfigure, "configure", false, "show configure, then exit")
	flag.BoolVar(&app.streamToStdout, "stream", false, "write to stdout while downloading")
	flag.UintVar(&app.StreamRate, "stream.rate", 12, "maximum number of stream rate (MiB/s)")
	flag.BoolVar(&app.ETagUnreliable, "etag.unreliable", false, "ignore unreliable ETag")
	flag.Parse()

	if workdir != "." {
		err := os.Chdir(workdir)
		if err != nil {
			println(err)
			return 1
		}
	}

	if !isFlagPassed("o") {
		app.OutFile = ""
	}

	os.Setenv("ConfigDir", filepath.Dir(conffile))

	client := http.DefaultClient

	if conffile != "" {
		err := app.loadConfigure(conffile)
		if err != nil && !os.IsNotExist(err) {
			if !isDir(conffile) || isFlagPassed("f") {
				println(err)
				return 1
			}
		}
		if err == nil {
			flag.Parse() // Command line flags take precedence.
		}
	}

	if flag.NArg() > 0 {
		app.URL = flag.Arg(0)
	}

	if showConfigure {
		app.showConfigure()
		return 2
	}

	if !showStatus {
		if app.URL == "" {
			if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) != 0 {
				print("Enter url: ")
			}
			stdin := bufio.NewScanner(os.Stdin)
			if !stdin.Scan() {
				return 1
			}
			app.URL = strings.TrimSpace(stdin.Text())
			if app.URL == "" {
				return 1
			}
		}
		if u, err := url.Parse(app.URL); err != nil {
			println(err)
			return 1
		} else if u.Scheme != "http" && u.Scheme != "https" {
			printf("unsupported protocol scheme \"%v\"\n", u.Scheme)
			return 1
		}
		if app.CookieFile != "" {
			cookiefile := os.ExpandEnv(app.CookieFile)
			jar, err := loadCookies(cookiefile)
			if err != nil && !os.IsNotExist(err) {
				println(err)
				return 1
			}
			if err == nil {
				client = &http.Client{Jar: jar}
			}
		}
	}

	filename := os.ExpandEnv(app.OutFile)
	if filename == "" {
		i := strings.LastIndexAny(app.URL, "/\\?#")
		if i != -1 && (app.URL[i] == '/' || app.URL[i] == '\\') {
			filename = app.URL[i+1:]
		}
		if filename == "" {
			filename = defaultOutFile
		}
	}

	filesize := int64(0)
	fileexists := false

	switch stat, err := os.Stat(filename); {
	case err == nil:
		filesize = stat.Size()
		fileexists = true
	case !os.IsNotExist(err) || showStatus:
		println(err)
		return 1
	}

	file, err := openDataFile(filename)
	if err != nil {
		println(err)
		return 1
	}
	defer func() {
		completeSize := file.CompleteSize()
		err := file.Close()
		if err != nil {
			println(err)
		}
		if completeSize == 0 && !fileexists {
			os.Remove(filename)
		}
	}()

	if filesize > 0 {
		if err := file.LoadHashFile(); err != nil {
			if os.IsNotExist(err) {
				filename := filename
				if !filepath.IsAbs(filename) {
					filename = filepath.Join(workdir, filename)
				}
				printf("\"%v\" already exists.\n", filename)
				println("If you do want to redownload it, remove it first.")
				return 1
			}
			println(err)
			return 1
		}
	}

	if showStatus {
		app.showStatus(file)
		return 2
	}

	if app.RequestRange != "" {
		var requestRange RangeSet
		for _, r := range strings.Split(app.RequestRange, ",") {
			r := strings.Split(r, "-")
			if len(r) > 2 {
				println("request range is invalid")
				return 1
			}
			i, err := strconv.ParseInt(r[0], 10, 32)
			if err != nil {
				println("request range is invalid")
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
				println("request range is invalid")
				return 1
			}
			requestRange.AddRange(i*1024*1024, (j+1)*1024*1024)
		}
		if len(requestRange) > 0 {
			file.SetRange(requestRange)
		}
	}

	mainCtx, mainCancel := context.WithCancel(context.Background())
	mainDone := mainCtx.Done()
	defer mainCancel()
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(interrupt)
		select {
		case <-interrupt:
			mainCancel()
		case <-mainDone:
		}
	}()

	var (
		streamRest chan struct{}
		streamDone chan struct{}
	)
	if app.streamToStdout {
		streamRest = make(chan struct{})
		streamDone = make(chan struct{})
		defer func() {
			<-streamDone
		}()
		go func() {
			defer close(streamDone)
			streamInterval := time.Second / 12
			if app.StreamRate > 0 {
				streamInterval = time.Second / time.Duration(app.StreamRate)
			}
			streamBuffer := make([]byte, 1024*1024)
			streamOffset := int64(0)
			streamTicker := time.NewTicker(streamInterval)
			defer streamTicker.Stop()
			for {
				select {
				case <-mainDone:
					return
				case <-streamTicker.C:
					if n, ok := file.ReadAt(streamBuffer, streamOffset); ok {
						streamOffset += int64(n)
						if _, err := os.Stdout.Write(streamBuffer[:n]); err != nil {
							return
						}
					}
				case <-streamRest:
					contentSize := file.ContentSize()
					for streamOffset < contentSize {
						if n, ok := file.ReadAt(streamBuffer, streamOffset); ok {
							streamOffset += int64(n)
							if _, err := os.Stdout.Write(streamBuffer[:n]); err != nil {
								return
							}
						} else {
							break
						}
						select {
						case <-mainDone:
							return
						default:
						}
					}
					return
				}
			}
		}()
	}

	for i := 0; i < 2; i++ {
		contentSize := file.ContentSize()
		completeSize := file.CompleteSize()
		if contentSize == 0 || completeSize != contentSize {
			app.dl(mainCtx, file, client)
			contentSize = file.ContentSize()
			completeSize = file.CompleteSize()
			if contentSize == 0 || completeSize != contentSize {
				return 1
			}
		}
		if app.streamToStdout {
			print("streaming...")
			app.streamToStdout = false
			close(streamRest)
			select {
			case <-mainDone:
				return 1
			case <-streamDone:
			}
			print("\033[1K\r")
		}

		var digest hash.Hash

		contentMD5 := strings.ToLower(file.ContentMD5())
		if len(contentMD5) == 32 {
			digest = md5.New()
			println("Content-MD5:", contentMD5)
		}

		contentDisposition := file.ContentDisposition()
		if contentDisposition != "" {
			println("Content-Disposition:", contentDisposition)
		}

		shouldVerify := i == 0 || digest != nil
		if !shouldVerify {
			return 0
		}

		p := int64(0)
		s := int64(0)
		w := func(b []byte) (n int, err error) {
			n = len(b)
			if digest != nil {
				n, err = digest.Write(b)
			}
			s += int64(n)
			if s*100 >= (p+1)*contentSize {
				p = s * 100 / contentSize
				print("\033[1K\r")
				printf("verifying...%v%%", p)
			}
			select {
			case <-mainDone:
				if err == nil {
					err = context.Canceled
				}
			default:
			}
			return
		}

		printf("verifying...%v%%", p)
		err := file.Verify(WriterFunc(w))
		print("\033[1K\r")
		print("verifying...")
		if err != nil {
			println(err)
			return 1
		}

		if file.CompleteSize() != contentSize {
			println("BAD")
			continue
		}

		if digest != nil && hex.EncodeToString(digest.Sum(nil)) != contentMD5 {
			println("BAD")
			return 1
		}

		println("OK")
		return 0
	}

	return 1
}

func (app *App) loadConfigure(name string) error {
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := yaml.NewDecoder(file)
	dec.SetStrict(true)

	c := app.Configure
	if err := dec.Decode(&c); err != nil {
		return err
	}

	app.Configure = c
	return nil
}

func (app *App) dl(mainCtx context.Context, file *DataFile, client *http.Client) {
	type (
		PrintMessage struct{}

		ResponseMessage struct {
			URL string
		}

		ProgressMessage struct {
			Just int
		}

		CompleteMessage struct {
			Err       error
			Fatal     bool
			Responsed bool
		}
	)

	var (
		firstRecvTime  time.Time
		nextReportTime time.Time
		totalReceived  int64
		recentReceived int64
		emaSpeed       int64
		connections    int
	)

	reportStatus := func() {
		timeUsed := time.Since(firstRecvTime)
		print("\033[1K\r")
		log.Printf(
			"recv %vB in %v, %vB/s\n",
			formatBytes(totalReceived),
			timeUsed.Round(time.Second),
			formatBytes(int64(float64(totalReceived)/timeUsed.Seconds())),
		)
	}

	var operators observable.Operators

	dlCtx := context.Background() // dlCtx never cancels.

	bytesPerSecond := observable.NewSubject()
	_, speedUpdateCancel := observable.Create(
		func(ctx context.Context, sink observable.Observer) (context.Context, context.CancelFunc) {
			const N = 5
			skipZeros := bytesPerSecond.Pipe(
				operators.SkipWhile(
					func(val interface{}, idx int) bool {
						return val.(int64) == 0
					},
				),
			)
			skipZeros.Pipe(
				operators.Scan(
					func(acc, val interface{}, idx int) interface{} {
						return acc.(int64) + val.(int64)
					},
				),
				operators.Map(
					func(val interface{}, idx int) interface{} {
						// For the first N seconds, we average the speed.
						emaSpeed = val.(int64) / int64(idx+1)
						return val
					},
				),
				operators.Take(N),
			).Subscribe(ctx, observable.NopObserver)
			return skipZeros.Pipe(
				observable.BufferCountConfig{N, 1}.MakeFunc(),
				operators.Skip(1),
				operators.Map(
					func(val interface{}, idx int) interface{} {
						values := val.([]interface{})
						sum := int64(0)
						for _, v := range values {
							sum += v.(int64)
						}
						return sum / int64(len(values))
					},
				),
				operators.Map(
					func(val interface{}, idx int) interface{} {
						// After N seconds, we calculate the speed by using
						// exponential moving average (EMA).
						const ia = 5 // Inverse of alpha.
						emaSpeed = (val.(int64) + (ia-1)*emaSpeed) / ia
						return val
					},
				),
				operators.TakeWhile(
					func(val interface{}, idx int) bool {
						return val.(int64) > 0
					},
				),
				operators.Finally(
					func() {
						emaSpeed = 0
					},
				),
			).Subscribe(ctx, sink)
		},
	).Pipe(
		operators.Repeat(-1),
	).Subscribe(dlCtx, observable.NopObserver)
	defer speedUpdateCancel()

	queuedMessages := observable.NewSubject()
	queuedMessagesCtx, _ := queuedMessages.
		Pipe(operators.Congest(int(app.MaxConnections*3))).
		Subscribe(dlCtx, func(t observable.Notification) {
			shouldPrint := false
			switch {
			case t.HasValue:
				switch v := t.Value.(type) {
				case ProgressMessage:
					if totalReceived == 0 {
						firstRecvTime = time.Now()
						nextReportTime = firstRecvTime.Add(reportInterval)
						log.Println("first arrival")
					}
					totalReceived += int64(v.Just)
					recentReceived += int64(v.Just)

				case PrintMessage:
					bytesPerSecond.Next(recentReceived)
					recentReceived = 0
					shouldPrint = totalReceived > 0

				case ResponseMessage:
					connections++

				case CompleteMessage:
					if v.Responsed {
						connections--
					}

				case string:
					print("\033[1K\r")
					log.Println(v)
					shouldPrint = totalReceived > 0
				}
			default:
				shouldPrint = totalReceived > 0
			}
			if shouldPrint {
				print("\033[1K\r")

				contentSize := file.ContentSize()
				completeSize := file.CompleteSize()

				{
					const length = 20
					progress := 0
					if contentSize > 0 {
						progress = int(float64(completeSize) / float64(contentSize) * 100)
					}
					s := strings.Repeat("=", length*progress/100)
					if len(s) < length && completeSize > 0 {
						s += ">"
					}
					if len(s) < length {
						s += strings.Repeat("-", length-len(s))
					}
					printf("%v%% [%v] ", progress, s)
				}

				printf("CN:%v DL:%vB/s", connections, formatBytes(emaSpeed))

				if contentSize > 0 && emaSpeed > 0 {
					remaining := float64(contentSize - completeSize)
					seconds := int64(math.Ceil(remaining / float64(emaSpeed)))
					printf(" ETA:%v", formatDuration(time.Duration(seconds)*time.Second))
				}

				if !t.HasValue {
					// about to exit, keep this status line
					println()
				}
			}
			if totalReceived > 0 && (!t.HasValue || time.Now().After(nextReportTime)) {
				nextReportTime = nextReportTime.Add(reportInterval)
				reportStatus()
			}
		})
	defer func() {
		queuedMessages.Complete()
		<-queuedMessagesCtx.Done()
	}()

	{
		_, cancel := observable.Interval(time.Second).
			Pipe(operators.MapTo(PrintMessage{})).
			Subscribe(dlCtx, queuedMessages.Observer)
		defer cancel()
	}

	queuedWrites := observable.NewSubject()
	queuedWritesCtx, _ := queuedWrites.
		Pipe(operators.Congest(int(app.MaxConnections*3))).
		Subscribe(dlCtx, func(t observable.Notification) {
			if t.HasValue {
				t.Value.(func())()
			}
		})
	defer func() {
		queuedWrites.Complete()
		<-queuedWritesCtx.Done()
		file.Sync()
	}()

	bufferPool := sync.Pool{
		New: func() interface{} {
			buf := make([]byte, readBufferSize)
			return &buf
		},
	}

	readAndWrite := func(body io.Reader, offset int64) (n int, err error) {
		b := bufferPool.Get().(*[]byte)
		n, err = body.Read(*b)
		if n > 0 {
			queuedWrites.Next(func() {
				file.WriteAt((*b)[:n], offset)
				bufferPool.Put(b)
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
		splitSize := int64(app.SplitSize) * 1024 * 1024
		if file.ContentSize() > 0 {
			size := (file.ContentSize() - file.CompleteSize()) / int64(app.MaxConnections)
			if size < splitSize || splitSize == 0 {
				splitSize = size
			}
		}
		return file.TakeIncomplete(splitSize)
	}

	returnIncomplete := func(offset, size int64) {
		file.ReturnIncomplete(offset, size)
	}

	messages := make(chan interface{}, app.MaxConnections)

	activeTasks := observable.NewSubject()
	activeTasksCtx, _ := activeTasks.
		Pipe(operators.MergeAll()).
		Subscribe(dlCtx, queuedMessages.Observer)
	defer func() {
		activeTasks.Complete()
		done := activeTasksCtx.Done()
		for {
			select {
			case <-done:
				return
			case <-messages:
			}
		}
	}()

	var (
		activeCount  uint
		pauseNewTask bool
		delayNewTask <-chan time.Time
		errorCount   uint
		fatalErrors  bool
		maxDownloads = app.MaxConnections
		currentURL   = app.URL
	)

	var (
		userAgents           = app.UserAgents
		userAgentIndex       = 0
		userAgentConnections = uint(0)
		newUserAgents        = make([]string, 0, len(app.UserAgents))
	)

	syncTicker := time.NewTicker(syncInterval)
	defer syncTicker.Stop()

	mainDone := mainCtx.Done()

	for {
		switch {
		case activeCount >= maxDownloads:
		case pauseNewTask:
		case delayNewTask != nil:
		case fatalErrors:
			if activeCount == 0 {
				return
			}
		case errorCount >= app.MaxErrors:
			if len(userAgents) > 1 {
				// If multiple user agents are provided, we are going to
				// test all of them one by one.
				userAgents = userAgents[1:] // Switch to next one.
				userAgentIndex = (userAgentIndex + 1) % len(app.UserAgents)
				userAgentConnections = 0
			} else {
				if len(app.UserAgents) > 1 {
					// We tested all user agents, now prepare to start over again.
					userAgentIndex = (userAgentIndex + 1) % len(app.UserAgents)
					userAgentConnections = 0
					userAgents = newUserAgents
					userAgents = append(userAgents, app.UserAgents[userAgentIndex:]...)
					userAgents = append(userAgents, app.UserAgents[:userAgentIndex]...)
				}
				if activeCount == 0 {
					// We tested all user agents, failed to start any download.
					return // Give up.
				}
				if currentURL == app.URL {
					// We tested all user agents and successfully started
					// some downloads, but now we are going to assume this
					// is all we can do so far.
					// Let's limit the number of parallel downloads.
					maxDownloads = activeCount
					errorCount = 0
					break
				}
			}
			currentURL = app.URL
			maxDownloads = app.MaxConnections
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

			var userAgent string
			if len(userAgents) > 0 {
				userAgent = userAgents[0]
			}

			cr := func(parent context.Context, sink observable.Observer) (ctx context.Context, cancel context.CancelFunc) {
				// Note that parent will never be canceled, because
				// subscription to activeTasks uses context.Background().
				ctx, cancel = context.WithCancel(mainCtx) // Not parent.

				sink = observable.Finally(sink, cancel)

				var (
					err   error
					fatal bool
				)
				defer func() {
					if err != nil {
						returnIncomplete(offset, size)
						sink.Next(CompleteMessage{err, fatal, false})
						sink.Complete()
					}
				}()

				req, err := http.NewRequest(http.MethodGet, currentURL, nil)
				if err != nil {
					return
				}

				shouldLimitSize := false
				if file.ContentSize() > 0 {
					req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+size-1))
				} else {
					req.Header.Set("Range", fmt.Sprintf("bytes=%v-", offset))
					shouldLimitSize = true
				}

				if app.Referer != "" {
					req.Header.Set("Referer", app.Referer)
				}

				req.Header.Set("User-Agent", userAgent)

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
				switch file.ContentSize() {
				case contentLength:
				case 0:
					shouldSync = true
					file.SetContentSize(contentLength)
				default:
					err = errors.New("Content-Length mismatched")
					fatal = true
					return
				}

				contentMD5 := resp.Header.Get("Content-MD5")
				switch file.ContentMD5() {
				case contentMD5:
				case "":
					shouldSync = true
					file.SetContentMD5(contentMD5)
				default:
					err = errors.New("Content-MD5 mismatched")
					fatal = true
					return
				}

				contentDisposition := resp.Header.Get("Content-Disposition")
				switch file.ContentDisposition() {
				case contentDisposition:
				default:
					shouldSync = true
					file.SetContentDisposition(contentDisposition)
				}

				eTag := resp.Header.Get("ETag")
				switch file.EntityTag() {
				case eTag:
				case "":
					shouldSync = true
					file.SetEntityTag(eTag)
				default:
					if !app.ETagUnreliable {
						err = errors.New("ETag mismatched")
						fatal = true
						return
					}
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

				sink.Next(ResponseMessage{resp.Request.URL.String()})

				go func() (err error) {
					var (
						readTimer        = time.AfterFunc(readTimeout, cancel)
						shouldResetTimer bool
						shouldWaitWrite  bool
					)

					defer func() {
						resp.Body.Close()
						if shouldWaitWrite {
							waitWriteDone()
						}
						returnIncomplete(offset, size)
						sink.Next(CompleteMessage{err, false, true})
						sink.Complete()
					}()

					for {
						if shouldResetTimer {
							readTimer.Reset(readTimeout)
						}
						n, err := readAndWrite(body, offset)
						readTimer.Stop()
						shouldResetTimer = true
						if n > 0 {
							offset, size = offset+int64(n), size-int64(n)
							shouldWaitWrite = true
							sink.Next(ProgressMessage{n})
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
						messages <- t.Value

					case CompleteMessage:
						messages <- t.Value
					}
				}
			}

			activeCount++
			pauseNewTask = true
			delayNewTask = time.After(app.RequestInterval)

			activeTasks.Next(observable.Create(cr).Pipe(operators.Do(do)))
		}

		select {
		case <-mainDone:
			return
		case <-delayNewTask:
			delayNewTask = nil
		case e := <-messages:
			switch e := e.(type) {
			case ResponseMessage:
				pauseNewTask = false
				currentURL = e.URL // Save the redirected one if redirection happens.
				maxDownloads = app.MaxConnections
				errorCount = 0
				if len(app.UserAgents) > 1 {
					queuedMessages.Next(
						fmt.Sprintf(
							"UserAgent #%v: +1 connections",
							userAgentIndex+1,
						),
					)
					userAgentConnections++
					if userAgentConnections == app.PerUserAgentLimit {
						// We successfully started some downloads with current
						// user agent, let's try next.
						userAgentIndex = (userAgentIndex + 1) % len(app.UserAgents)
						userAgentConnections = 0
						userAgents = newUserAgents
						userAgents = append(userAgents, app.UserAgents[userAgentIndex:]...)
						userAgents = append(userAgents, app.UserAgents[:userAgentIndex]...)
						// If we change the user agent, we probably need to
						// reset the url to the original one.
						currentURL = app.URL
					}
				}
			case CompleteMessage:
				activeCount--
				if !e.Responsed {
					pauseNewTask = false
					errorCount++ // There must be an error if no response.
				}
				if e.Err != nil && e.Fatal {
					fatalErrors = true
				}
				if e.Responsed && len(app.UserAgents) > 1 {
					// Prepare to test all user agents again.
					userAgents = newUserAgents
					userAgents = append(userAgents, app.UserAgents[userAgentIndex:]...)
					userAgents = append(userAgents, app.UserAgents[:userAgentIndex]...)
				}
				if e.Err != nil && !e.Responsed {
					unwrappedErr := e.Err
					switch e := unwrappedErr.(type) {
					case *net.OpError:
						unwrappedErr = e.Err
					case *url.Error:
						unwrappedErr = e.Err
					}
					message := unwrappedErr.Error()
					if len(app.UserAgents) > 1 {
						message = fmt.Sprintf(
							"UserAgent #%v: %v",
							userAgentIndex+1,
							message,
						)
					}
					queuedMessages.Next(message)
				}
			}
		case <-syncTicker.C:
			file.Sync()
		}
	}
}

func (app *App) showStatus(file *DataFile) {
	var (
		contentSize        = file.ContentSize()
		completeSize       = file.CompleteSize()
		contentMD5         = file.ContentMD5()
		contentDisposition = file.ContentDisposition()
		entityTag          = file.EntityTag()
	)
	if contentSize > 0 {
		progress := int(float64(completeSize) / float64(contentSize) * 100)
		fmt.Println("Size:", contentSize)
		fmt.Println("Completed:", completeSize, fmt.Sprintf("(%v%%)", progress))
	} else {
		fmt.Println("Size: unknown")
		fmt.Println("Completed:", completeSize)
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
	if contentMD5 != "" {
		fmt.Println("Content-MD5:", contentMD5)
	}
	if contentDisposition != "" {
		fmt.Println("Content-Disposition:", contentDisposition)
	}
	if entityTag != "" {
		fmt.Println("ETag:", entityTag)
	}
}

func (app *App) showConfigure() {
	enc := yaml.NewEncoder(os.Stdout)
	enc.Encode(&app.Configure)
	enc.Close()
}

func print(a ...interface{}) {
	fmt.Fprint(os.Stderr, a...)
}

func printf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func println(a ...interface{}) {
	fmt.Fprintln(os.Stderr, a...)
}

func formatBytes(n int64) string {
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

func formatDuration(d time.Duration) (s string) {
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

func isDir(name string) bool {
	stat, err := os.Stat(name)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

func isFlagPassed(name string) (yes bool) {
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			yes = true
		}
	})
	return
}

func loadCookies(name string) (jar http.CookieJar, err error) {
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

		if jar == nil {
			jar, err = cookiejar.New(&cookiejar.Options{
				PublicSuffixList: publicsuffix.List,
			})
			if err != nil {
				return nil, err
			}
		}
		jar.SetCookies(
			&url.URL{Scheme: scheme, Host: host},
			[]*http.Cookie{&cookie},
		)
	}
	return
}
