package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/net/publicsuffix"
	"gopkg.in/yaml.v2"
)

const (
	readBufferSize = 4096
	reportInterval = 10 * time.Minute
)

type App struct {
	Configure
	workdir        string
	conffile       string
	showStatus     bool
	showConfigure  bool
	streamToStdout bool
}

type Configure struct {
	Alloc                 bool          `mapstructure:"alloc" yaml:"alloc"`
	Connections           uint          `mapstructure:"connections" yaml:"connections"`
	CookieFile            string        `mapstructure:"cookie" yaml:"cookie"`
	DialTimeout           time.Duration `mapstructure:"dial-timeout" yaml:"dial-timeout"`
	Errors                uint          `mapstructure:"errors" yaml:"errors"`
	Interval              time.Duration `mapstructure:"interval" yaml:"interval"`
	KeepAlive             time.Duration `mapstructure:"keep-alive" yaml:"keep-alive"`
	ListenAddress         string        `mapstructure:"listen" yaml:"listen"`
	OutputFile            string        `mapstructure:"output" yaml:"output"`
	PerUserAgentLimit     uint          `mapstructure:"per-user-agent-limit" yaml:"per-user-agent-limit"`
	Range                 string        `mapstructure:"range" yaml:"range"`
	ReadTimeout           time.Duration `mapstructure:"read-timeout" yaml:"read-timeout"`
	Referer               string        `mapstructure:"referer" yaml:"referer"`
	ResponseHeaderTimeout time.Duration `mapstructure:"response-header-timeout" yaml:"response-header-timeout"`
	SkipETag              bool          `mapstructure:"skip-etag" yaml:"skip-etag"`
	SkipLastModified      bool          `mapstructure:"skip-last-modified" yaml:"skip-last-modified"`
	SplitSize             uint          `mapstructure:"split" yaml:"split"`
	StreamRate            uint          `mapstructure:"stream-rate" yaml:"stream-rate"`
	SyncPeriod            time.Duration `mapstructure:"sync-period" yaml:"sync-period"`
	Timeout               time.Duration `mapstructure:"timeout" yaml:"timeout"`
	TLSHandshakeTimeout   time.Duration `mapstructure:"tls-handshake-timeout" yaml:"tls-handshake-timeout"`
	Truncate              bool          `mapstructure:"truncate" yaml:"truncate"`
	URL                   string        `mapstructure:"url" yaml:"url"`
	UserAgent             string        `mapstructure:"user-agent" yaml:"user-agent"`
	Verify                bool          `mapstructure:"verify" yaml:"verify"`
}

var operators observable.Operators

func main() {
	var app App

	rootCmd := &cobra.Command{
		Use:   "resume",
		Short: "splitting download a file",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			os.Exit(app.Main(cmd, args))
		},
	}

	flags := rootCmd.PersistentFlags()

	flags.BoolVar(&app.Alloc, "alloc", false, "alloc disk space before first write")
	flags.BoolVar(&app.Truncate, "truncate", false, "truncate output file before first write")
	flags.BoolVar(&app.Verify, "verify", true, "verify output file after download completes")
	flags.BoolVarP(&app.SkipETag, "skip-etag", "E", false, "skip unreliable ETag field")
	flags.BoolVarP(&app.SkipLastModified, "skip-last-modified", "M", false, "skip unreliable Last-Modified field")
	flags.DurationVar(&app.DialTimeout, "dial-timeout", 30*time.Second, "dial timeout")
	flags.DurationVar(&app.KeepAlive, "keep-alive", 30*time.Second, "keep-alive duration")
	flags.DurationVar(&app.ReadTimeout, "read-timeout", 30*time.Second, "read timeout")
	flags.DurationVar(&app.ResponseHeaderTimeout, "response-header-timeout", 10*time.Second, "response header timeout")
	flags.DurationVar(&app.SyncPeriod, "sync-period", 10*time.Minute, "sync-to-disk period")
	flags.DurationVar(&app.TLSHandshakeTimeout, "tls-handshake-timeout", 10*time.Second, "tls handshake timeout")
	flags.DurationVarP(&app.Interval, "interval", "i", 2*time.Second, "request interval")
	flags.DurationVarP(&app.Timeout, "timeout", "t", 0, "if non-zero, all timeouts default to this value")
	flags.StringVar(&app.CookieFile, "cookie", "", "cookie file")
	flags.StringVarP(&app.ListenAddress, "listen", "L", "", "HTTP listen address for remote control")
	flags.StringVarP(&app.OutputFile, "output", "o", "", "output file")
	flags.StringVarP(&app.Range, "range", "r", "", "request range (MiB), e.g., 0-1023")
	flags.StringVarP(&app.Referer, "referer", "R", "", "referer url")
	flags.StringVarP(&app.UserAgent, "user-agent", "A", "", "user agent")
	flags.UintVarP(&app.Connections, "connections", "c", 4, "maximum number of parallel downloads")
	flags.UintVarP(&app.Errors, "errors", "e", 3, "maximum number of errors")
	flags.UintVarP(&app.SplitSize, "split", "s", 0, "split size (MiB), 0 means use maximum reasonable")

	viper.BindPFlags(flags)

	flags.StringVarP(&app.workdir, "workdir", "w", ".", "working directory")
	flags.StringVarP(&app.conffile, "conf", "f", "resume.yaml", "configure file")

	rootCmd.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show status",
		Run: func(cmd *cobra.Command, args []string) {
			app.showStatus = true
			os.Exit(app.Main(cmd, args))
		},
	})
	rootCmd.AddCommand(&cobra.Command{
		Use:   "config",
		Short: "Show configure",
		Run: func(cmd *cobra.Command, args []string) {
			app.showConfigure = true
			os.Exit(app.Main(cmd, args))
		},
	})

	streamCmd := &cobra.Command{
		Use:   "stream",
		Short: "Write to stdout while downloading",
		Run: func(cmd *cobra.Command, args []string) {
			app.streamToStdout = true
			os.Exit(app.Main(cmd, args))
		},
	}
	{
		flags := streamCmd.PersistentFlags()
		flags.UintVar(&app.StreamRate, "rate", 12, "maximum number of stream rate (MiB/s)")
		viper.BindPFlag("stream-rate", flags.Lookup("rate"))
	}
	rootCmd.AddCommand(streamCmd)

	rootCmd.Execute()
}

func (app *App) Main(cmd *cobra.Command, args []string) int {
	log.SetFlags(log.Ltime)

	if app.workdir != "." {
		err := os.Chdir(app.workdir)
		if err != nil {
			println(err)
			return 1
		}
	}

	os.Setenv("ConfigDir", filepath.Dir(app.conffile))

	if app.conffile != "" {
		viper.SetConfigFile(app.conffile)
		viper.SetConfigType("yaml")
		if err := viper.ReadInConfig(); err != nil && !os.IsNotExist(err) {
			if !isDir(app.conffile) || cmd.Flags().Changed("conf") {
				println(err)
				return 1
			}
		}
		viper.Unmarshal(&app.Configure)
	}

	if app.Timeout > 0 {
		timeoutFlagProvided := cmd.Flags().Changed("timeout")
		if !cmd.Flags().Changed("dial-timeout") {
			if timeoutFlagProvided || !viper.InConfig("dial-timeout") {
				app.DialTimeout = app.Timeout
			}
		}
		if !cmd.Flags().Changed("read-timeout") {
			if timeoutFlagProvided || !viper.InConfig("read-timeout") {
				app.ReadTimeout = app.Timeout
			}
		}
		if !cmd.Flags().Changed("response-header-timeout") {
			if timeoutFlagProvided || !viper.InConfig("response-header-timeout") {
				app.ResponseHeaderTimeout = app.Timeout
			}
		}
		if !cmd.Flags().Changed("tls-handshake-timeout") {
			if timeoutFlagProvided || !viper.InConfig("tls-handshake-timeout") {
				app.TLSHandshakeTimeout = app.Timeout
			}
		}
	}

	if len(args) > 0 {
		app.URL = args[0]
	}

	if app.showConfigure {
		enc := yaml.NewEncoder(os.Stdout)
		enc.Encode(&app.Configure)
		enc.Close()
		return 0
	}

	var cookieJar http.CookieJar

	if !app.showStatus {
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
				cookieJar = jar
			}
		}
	}

	filename := os.ExpandEnv(app.OutputFile)
	if filename == "" {
		i := strings.LastIndexAny(app.URL, "/\\?#")
		if i != -1 && (app.URL[i] == '/' || app.URL[i] == '\\') {
			filename = app.URL[i+1:]
		}
		if filename == "" {
			filename = "resume.out"
		}
	}

	fileexists := false

	switch _, err := os.Stat(filename); {
	case err == nil:
		fileexists = true
	case !os.IsNotExist(err) || app.showStatus:
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

	if fileexists {
		if err := file.LoadHashFile(); err != nil {
			if os.IsNotExist(err) {
				filename := filename
				if !filepath.IsAbs(filename) {
					filename = filepath.Join(app.workdir, filename)
				}
				printf("\"%v\" already exists.\n", filename)
				println("If you do want to redownload it, remove it first.")
				return 1
			}
			println(err)
			return 1
		}
	}

	if app.showStatus {
		app.status(file, os.Stdout)
		return 0
	}

	if app.Range != "" {
		var sections RangeSet
		for _, r := range strings.Split(app.Range, ",") {
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
				sections.AddRange(i*1024*1024, (i+1)*1024*1024)
				continue
			}
			if r[1] == "" {
				sections.AddRange(i*1024*1024, math.MaxInt64)
				continue
			}
			j, err := strconv.ParseInt(r[1], 10, 32)
			if err != nil || j < i {
				println("request range is invalid")
				return 1
			}
			sections.AddRange(i*1024*1024, (j+1)*1024*1024)
		}
		if len(sections) > 0 {
			file.SetRange(sections)
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
					if n, err := file.ReadAt(streamBuffer, streamOffset); err == nil {
						streamOffset += int64(n)
						if _, err := os.Stdout.Write(streamBuffer[:n]); err != nil {
							return
						}
					}
				case <-streamRest:
					contentSize := file.ContentSize()
					for streamOffset < contentSize {
						if n, err := file.ReadAt(streamBuffer, streamOffset); err == nil {
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

	if app.ListenAddress != "" {
		l, err := net.Listen("tcp", app.ListenAddress)
		if err != nil {
			println(err)
			return 1
		}
		exitHandler := http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				mainCancel()
			},
		)
		http.Handle("/exit", exitHandler)
		http.Handle("/quit", exitHandler)
		http.Handle("/status", http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				app.status(file, w)
			},
		))
		streamNotify := observable.NewSubject()
		streamCounter := observable.NewBehaviorSubject(0)
		streamNotify.Pipe(
			operators.Scan(
				func(acc, val interface{}, idx int) interface{} {
					return acc.(int) + val.(int)
				},
			),
		).Subscribe(
			context.Background(),
			streamCounter.Observer,
		)
		http.Handle("/stream", http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				streamNotify.Next(1)
				defer streamNotify.Next(-1)
				done := r.Context().Done()
				currentOffset := int64(0)
				content := struct {
					io.Reader
					io.Seeker
				}{
					ReaderFunc(
						func(p []byte) (n int, err error) {
							n, err = file.ReadAt(p, currentOffset)
							for err == ErrIncomplete {
								select {
								case <-done:
									return
								case <-mainDone:
									return
								case <-time.After(time.Second):
									n, err = file.ReadAt(p, currentOffset)
								}
							}
							currentOffset += int64(n)
							return
						},
					),
					SeekerFunc(
						func(offset int64, whence int) (int64, error) {
							switch whence {
							case io.SeekStart:
								currentOffset = offset
							case io.SeekCurrent:
								currentOffset += offset
							case io.SeekEnd:
								currentOffset = file.ContentSize() - offset
							}
							return currentOffset, nil
						},
					),
				}
				http.ServeContent(w, r, filepath.Base(filename), time.Time{}, content)
			},
		))
		srv := &http.Server{}
		defer func() {
			if streamCounter.Value().(int) > 0 {
				// Wait until `streamCounter` remains zero for N seconds.
				const N = 5
				println("Waiting for remote streaming to complete...")
				streamCounter.Pipe(
					operators.Filter(
						func(val interface{}, idx int) bool {
							return val == 0
						},
					),
					operators.SwitchMapTo(
						observable.Race(
							observable.Interval(N*time.Second).Pipe(operators.MapTo(nil)),
							streamCounter.Pipe(
								operators.Exclude(
									func(val interface{}, idx int) bool {
										// Exclude the first value that is zero.
										return idx == 0 && val == 0
									},
								),
							),
						).Pipe(
							operators.Take(1),
							operators.Filter(
								func(val interface{}, idx int) bool {
									// Check if this is the value from `Interval`.
									return val == nil
								},
							),
						),
					),
					operators.Take(1),
				).BlockingSingle(mainCtx)
			}
			timeout := make(chan bool, 1)
			timeout <- true
			time.AfterFunc(time.Second, func() {
				if <-timeout {
					println("shuting down remote control server...")
					close(timeout)
				}
			})
			srv.Shutdown(mainCtx)
			if <-timeout {
				close(timeout)
			}
		}()
		go srv.Serve(l)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   app.DialTimeout,
				KeepAlive: app.KeepAlive,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   app.TLSHandshakeTimeout,
			ResponseHeaderTimeout: app.ResponseHeaderTimeout,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Jar: cookieJar,
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

		if !app.Verify {
			return 0
		}

		contentMD5 := file.ContentMD5()
		shouldVerify := i == 0 || contentMD5 != ""
		if !shouldVerify {
			return 0
		}

		var digest hash.Hash
		if contentMD5 != "" {
			println("Content-MD5:", contentMD5)
			digest = md5.New()
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
			return
		}

		printf("verifying...%v%%", p)
		err := file.Verify(mainCtx, WriterFunc(w))
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
			"recv %vB in %v, %vB/s, %v%% completed\n",
			formatBytes(totalReceived),
			strings.TrimSuffix(timeUsed.Round(time.Minute).String(), "0s"),
			formatBytes(int64(float64(totalReceived)/timeUsed.Seconds())),
			int(float64(file.CompleteSize())/float64(file.ContentSize())*100),
		)
	}

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
		Pipe(operators.Congest(int(app.Connections*3))).
		Subscribe(dlCtx, func(t observable.Notification) {
			shouldPrint := false
			switch {
			case t.HasValue:
				switch v := t.Value.(type) {
				case ProgressMessage:
					if totalReceived == 0 {
						firstRecvTime = time.Now()
						nextReportTime = firstRecvTime.Add(reportInterval)
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

				const length = 20
				progress := int(float64(completeSize) / float64(contentSize) * 100)
				s := strings.Repeat("=", length*progress/100)
				if len(s) < length {
					s += ">"
				}
				if len(s) < length {
					s += strings.Repeat("-", length-len(s))
				}
				printf("%v%% [%v] CN:%v DL:%vB/s", progress, s, connections, formatBytes(emaSpeed))

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
		Pipe(operators.Congest(int(app.Connections*3))).
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
			size := (file.ContentSize() - file.CompleteSize()) / int64(app.Connections)
			if size < splitSize || splitSize == 0 {
				splitSize = size
			}
		}
		return file.TakeIncomplete(splitSize)
	}

	returnIncomplete := func(offset, size int64) {
		file.ReturnIncomplete(offset, size)
	}

	messages := make(chan interface{}, app.Connections)

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
		maxDownloads = app.Connections
		currentURL   = app.URL
	)

	var (
		allUserAgents  = strings.Split(strings.TrimSuffix(app.UserAgent, "\n"), "\n")
		newUserAgents  = make([]string, 0, len(allUserAgents))
		userAgents     = allUserAgents
		userAgentIndex = 0
		userAgentConns = uint(0)
	)

	syncPeriod := app.SyncPeriod
	if syncPeriod < time.Minute {
		syncPeriod = time.Minute
	}

	syncTicker := time.NewTicker(syncPeriod)
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
		case errorCount >= app.Errors:
			if len(userAgents) > 1 {
				// If multiple user agents are provided, we are going to
				// test all of them one by one.
				userAgents = userAgents[1:] // Switch to next one.
				userAgentIndex = (userAgentIndex + 1) % len(allUserAgents)
				userAgentConns = 0
			} else {
				if len(allUserAgents) > 1 {
					// We tested all user agents, now prepare to start over again.
					userAgentIndex = (userAgentIndex + 1) % len(allUserAgents)
					userAgentConns = 0
					userAgents = newUserAgents
					userAgents = append(userAgents, allUserAgents[userAgentIndex:]...)
					userAgents = append(userAgents, allUserAgents[:userAgentIndex]...)
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
			maxDownloads = app.Connections
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

				shouldAlloc := false
				shouldSync := false

				contentSize, _ := strconv.ParseInt(slice[3], 10, 64)
				switch file.ContentSize() {
				case contentSize:
				case 0:
					file.SetContentSize(contentSize)
					shouldAlloc = app.Alloc
					shouldSync = true
				default:
					err = errors.New("Content-Length mismatched")
					fatal = true
					return
				}

				contentDisposition := resp.Header.Get("Content-Disposition")
				if contentDisposition != "" {
					if contentDisposition != file.ContentDisposition() {
						file.SetContentDisposition(contentDisposition)
						shouldSync = true
					}
				}

				contentMD5 := strings.ToLower(resp.Header.Get("Content-MD5"))
				if len(contentMD5) == 32 {
					switch file.ContentMD5() {
					case contentMD5:
					case "":
						file.SetContentMD5(contentMD5)
						shouldSync = true
					default:
						err = errors.New("Content-MD5 mismatched")
						fatal = true
						return
					}
				}

				eTag := resp.Header.Get("ETag")
				if eTag != "" {
					switch file.EntityTag() {
					case eTag:
					case "":
						file.SetEntityTag(eTag)
						shouldSync = true
					default:
						if !app.SkipETag {
							err = errors.New("ETag mismatched")
							fatal = true
							return
						}
					}
				}

				lastModified := resp.Header.Get("Last-Modified")
				if lastModified != "" {
					switch file.LastModified() {
					case lastModified:
					case "":
						file.SetLastModified(lastModified)
						shouldSync = true
					default:
						if !app.SkipLastModified {
							err = errors.New("Last-Modified mismatched")
							fatal = true
							return
						}
					}
				}

				if shouldAlloc {
					err = app.alloc(mainCtx, file)
					if err != nil {
						fatal = true
						return
					}
				}

				if app.Truncate {
					file.Truncate(contentSize)
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
						readTimeout      = app.ReadTimeout
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
			delayNewTask = time.After(app.Interval)

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
				maxDownloads = app.Connections
				errorCount = 0
				if len(allUserAgents) > 1 {
					queuedMessages.Next(
						fmt.Sprintf(
							"UserAgent #%v: +1 connections",
							userAgentIndex+1,
						),
					)
					userAgentConns++
					switch userAgentConns {
					case app.PerUserAgentLimit:
						// We successfully started some downloads with current
						// user agent, let's try next.
						userAgentIndex = (userAgentIndex + 1) % len(allUserAgents)
						userAgentConns = 0
						userAgents = newUserAgents
						userAgents = append(userAgents, allUserAgents[userAgentIndex:]...)
						userAgents = append(userAgents, allUserAgents[:userAgentIndex]...)
						// If we change the user agent, we probably need to
						// reset the url to the original one.
						currentURL = app.URL
					case 1:
						// Successfully made the first connection with current
						// user agent, we'll test all other user agents again.
						userAgents = newUserAgents
						userAgents = append(userAgents, allUserAgents[userAgentIndex:]...)
						userAgents = append(userAgents, allUserAgents[:userAgentIndex]...)
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
				if e.Responsed && len(allUserAgents) > 1 {
					// Prepare to test all user agents again.
					userAgents = newUserAgents
					userAgents = append(userAgents, allUserAgents[userAgentIndex:]...)
					userAgents = append(userAgents, allUserAgents[:userAgentIndex]...)
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
					if len(allUserAgents) > 1 {
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

func (app *App) alloc(mainCtx context.Context, file *DataFile) error {
	done := make(chan struct{})
	defer func() {
		<-done
		print("\033[1K\r")
	}()
	progress := make(chan int64, 1)
	defer close(progress)
	contentSize := file.ContentSize()
	go func() {
		p := int64(0)
		print("\033[1K\r")
		printf("allocating...%v%%", p)
		for s := range progress {
			if s*100 >= (p+1)*contentSize {
				p = s * 100 / contentSize
				print("\033[1K\r")
				printf("allocating...%v%%", p)
			}
		}
		close(done)
	}()
	return file.Alloc(mainCtx, progress)
}

func (app *App) status(file *DataFile, writer io.Writer) {
	var (
		contentSize        = file.ContentSize()
		completeSize       = file.CompleteSize()
		contentDisposition = file.ContentDisposition()
		contentMD5         = file.ContentMD5()
		entityTag          = file.EntityTag()
	)
	if contentSize > 0 {
		progress := int(float64(completeSize) / float64(contentSize) * 100)
		fmt.Fprintln(writer, "Size:", contentSize)
		fmt.Fprintln(writer, "Completed:", completeSize, fmt.Sprintf("(%v%%)", progress))
	} else {
		fmt.Fprintln(writer, "Size: unknown")
		fmt.Fprintln(writer, "Completed:", completeSize)
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
		fmt.Fprintln(writer, "Incomplete(MiB):", strings.Join(items, ","))
	}
	if contentDisposition != "" {
		fmt.Fprintln(writer, "Content-Disposition:", contentDisposition)
	}
	if contentMD5 != "" {
		fmt.Fprintln(writer, "Content-MD5:", contentMD5)
	}
	if entityTag != "" {
		fmt.Fprintln(writer, "ETag:", entityTag)
	}
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
