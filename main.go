package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/b97tsk/async"
	"github.com/b97tsk/intervals"
	"github.com/b97tsk/intervals/elems"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/net/publicsuffix"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"
)

func main() {
	var app App

	rootCmd := &cobra.Command{
		Use:   "resume",
		Short: "A resumable multipart HTTP downloader.",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			os.Exit(app.Main(cmd, args))
		},
	}

	must := func(err error) {
		if err != nil {
			println("fatal:", err)
			os.Exit(exitCodeFatal)
		}
	}

	flags := rootCmd.PersistentFlags()

	flags.BoolVar(&app.Alloc, "alloc", false, "alloc disk space before the first write")
	flags.BoolVar(&app.Autoremove, "autoremove", true, "auto remove .resume file after successfully verified")
	flags.BoolVar(&app.Truncate, "truncate", false, "truncate output file before the first write")
	flags.BoolVar(&app.Verify, "verify", true, "verify output file after download completes")
	flags.BoolVarP(&app.DisableKeepAlives, "disable-keep-alives", "D", false, "disable HTTP keep alives")
	flags.BoolVarP(&app.IgnoreErrors, "ignore-errors", "g", false, "ignore non-fatal errors")
	flags.BoolVarP(&app.SkipETag, "skip-etag", "E", false, "skip unreliable ETag field")
	flags.BoolVarP(&app.SkipLastModified, "skip-last-modified", "M", false, "skip unreliable Last-Modified field")
	flags.BoolVarP(&app.TimeoutIntolerant, "timeout-intolerant", "T", false, "treat timeouts as errors")
	flags.BoolVarP(&app.Verbose, "verbose", "v", false, "write additional information to stderr")
	flags.DurationVar(&app.KeepAlive, "keep-alive", 30*time.Second, "keep-alive duration")
	flags.DurationVar(&app.ReadTimeout, "read-timeout", 30*time.Second, "read timeout")
	flags.DurationVar(&app.ResponseHeaderTimeout, "response-header-timeout", 10*time.Second, "response header timeout")
	flags.DurationVar(&app.SyncPeriod, "sync-period", 10*time.Minute, "sync-to-disk period")
	flags.DurationVar(&app.TLSHandshakeTimeout, "tls-handshake-timeout", 10*time.Second, "tls handshake timeout")
	flags.DurationVarP(&app.Interval, "interval", "i", 2*time.Second, "request interval")
	flags.DurationVarP(&app.Timeout, "timeout", "t", 30*time.Second, "timeout for all, low priority")
	flags.StringVar(&app.CookieFile, "cookie", "", "cookie file")
	flags.StringVarP(&app.LimitRate, "limit-rate", "l", "", "limit download rate to this value (B/s), e.g., 32K")
	flags.StringVarP(&app.ListenAddress, "listen", "L", "", "HTTP listen address for remote control")
	flags.StringVarP(&app.MinDesiredRate, "min-desired-rate", "d", "", "minimum desired download rate (B/s), e.g., 32K")
	flags.StringVarP(&app.OutputFile, "output", "o", "", "output file")
	flags.StringVarP(&app.Proxy, "proxy", "x", "", "a shorthand for setting http(s)_proxy environment variables")
	flags.StringVarP(&app.Range, "range", "r", "", "request range (MiB), e.g., 0-1023")
	flags.StringVarP(&app.Referer, "referer", "e", "", "referer url")
	flags.StringVarP(&app.UserAgent, "user-agent", "A", "", "user agent")
	flags.UintVarP(&app.Connections, "connections", "c", 1, "maximum number of parallel downloads")
	flags.UintVarP(&app.MaxSplitSize, "max-split", "s", 0, "maximum split size (MiB), 0 means use maximum possible")
	flags.UintVarP(&app.MinSplitSize, "min-split", "p", 0, "minimum split size (MiB), even smaller value may be used")
	flags.UintVarP(&app.Parallel, "parallel", "P", 0, "maximum number of parallel requests (default =connections)")

	must(viper.BindPFlags(flags))

	flags.BoolVarP(&app.retryForever, "retry-forever", "Y", false, "retry forever")
	flags.BoolVarP(&app.noUserConfig, "no-user-config", "n", false, "do not load .resumerc file")
	flags.StringVarP(&app.workdir, "workdir", "w", ".", "working directory")
	flags.StringVarP(&app.conffile, "config", "f", "resume.yaml", "configuration file")
	flags.UintVarP(&app.retryCount, "retry", "y", 0, "retry count")

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
		Short: "Show configuration",
		Run: func(cmd *cobra.Command, args []string) {
			app.showConfig = true
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
		flags.UintVarP(&app.StreamCache, "cache", "k", 0, "stream cache size (MiB), 0 means unlimited")
		must(viper.BindPFlag("stream-rate", flags.Lookup("rate")))
		must(viper.BindPFlag("stream-cache", flags.Lookup("cache")))
	}
	rootCmd.AddCommand(streamCmd)

	verifyCmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify downloaded parts",
		Run: func(cmd *cobra.Command, args []string) {
			app.verifyOnly = true
			os.Exit(app.Main(cmd, args))
		},
	}
	{
		flags := verifyCmd.PersistentFlags()
		flags.BoolVar(&app.fixCorrupted, "fix", false, "mark corrupted parts as not downloaded")
	}
	rootCmd.AddCommand(verifyCmd)

	_ = rootCmd.Execute()
}

type Configuration struct {
	Alloc                 bool          `mapstructure:"alloc" yaml:"alloc"`
	Autoremove            bool          `mapstructure:"autoremove" yaml:"autoremove"`
	Connections           uint          `mapstructure:"connections" yaml:"connections"`
	CookieFile            string        `mapstructure:"cookie" yaml:"cookie"`
	DisableKeepAlives     bool          `mapstructure:"disable-keep-alives" yaml:"disable-keep-alives"`
	IgnoreErrors          bool          `mapstructure:"ignore-errors" yaml:"ignore-errors"`
	Interval              time.Duration `mapstructure:"interval" yaml:"interval"`
	KeepAlive             time.Duration `mapstructure:"keep-alive" yaml:"keep-alive"`
	LimitRate             string        `mapstructure:"limit-rate" yaml:"limit-rate"`
	ListenAddress         string        `mapstructure:"listen" yaml:"listen"`
	MaxSplitSize          uint          `mapstructure:"max-split" yaml:"max-split"`
	MinDesiredRate        string        `mapstructure:"min-desired-rate" yaml:"min-desired-rate"`
	MinSplitSize          uint          `mapstructure:"min-split" yaml:"min-split"`
	OutputFile            string        `mapstructure:"output" yaml:"output"`
	Parallel              uint          `mapstructure:"parallel" yaml:"parallel"`
	Proxy                 string        `mapstructure:"proxy" yaml:"proxy"`
	Range                 string        `mapstructure:"range" yaml:"range"`
	ReadTimeout           time.Duration `mapstructure:"read-timeout" yaml:"read-timeout"`
	Referer               string        `mapstructure:"referer" yaml:"referer"`
	ResponseHeaderTimeout time.Duration `mapstructure:"response-header-timeout" yaml:"response-header-timeout"`
	SkipETag              bool          `mapstructure:"skip-etag" yaml:"skip-etag"`
	SkipLastModified      bool          `mapstructure:"skip-last-modified" yaml:"skip-last-modified"`
	StreamCache           uint          `mapstructure:"stream-cache" yaml:"stream-cache"`
	StreamRate            uint          `mapstructure:"stream-rate" yaml:"stream-rate"`
	SyncPeriod            time.Duration `mapstructure:"sync-period" yaml:"sync-period"`
	Timeout               time.Duration `mapstructure:"timeout" yaml:"timeout"`
	TimeoutIntolerant     bool          `mapstructure:"timeout-intolerant" yaml:"timeout-intolerant"`
	TLSHandshakeTimeout   time.Duration `mapstructure:"tls-handshake-timeout" yaml:"tls-handshake-timeout"`
	Truncate              bool          `mapstructure:"truncate" yaml:"truncate"`
	URL                   string        `mapstructure:"url" yaml:"url"`
	UserAgent             string        `mapstructure:"user-agent" yaml:"user-agent"`
	Verbose               bool          `mapstructure:"verbose" yaml:"verbose"`
	Verify                bool          `mapstructure:"verify" yaml:"verify"`
}

type App struct {
	Configuration
	workdir        string
	conffile       string
	noUserConfig   bool
	showStatus     bool
	showConfig     bool
	verifyOnly     bool
	fixCorrupted   bool
	streamToStdout bool
	minDesiredRate int64
	retryCount     uint
	retryForever   bool

	file         *DataFile
	client       *http.Client
	canRequest   <-chan struct{}
	rateLimiter  *rate.Limiter
	streamOffset atomic.Int64

	dl struct {
		sync.WaitGroup
		async.Executor
	}

	fws struct {
		sync.WaitGroup
		async.Executor
	}

	buf struct {
		sync.Pool
		sem chan struct{}
	}

	statusLine       string
	statusLineBuffer bytes.Buffer
}

func (app *App) Main(cmd *cobra.Command, args []string) int {
	if app.workdir != "." {
		err := os.Chdir(app.workdir)
		if err != nil {
			println("fatal:", err)
			return exitCodeFatal
		}
	}

	os.Setenv("ConfigDir", filepath.Dir(app.conffile))

	if !app.noUserConfig {
		if dir, err := os.UserHomeDir(); err == nil {
			viper.SetConfigFile(filepath.Join(dir, ".resumerc"))
			viper.SetConfigType("yaml")

			if err := viper.ReadInConfig(); err == nil {
				_ = viper.Unmarshal(&app.Configuration)
			}
		}
	}

	if app.conffile != "" {
		viper.SetConfigFile(app.conffile)
		viper.SetConfigType("yaml")

		if err := viper.ReadInConfig(); err != nil {
			if cmd.Flags().Changed("conf") || !os.IsNotExist(err) && !isDir(app.conffile) {
				println(err)
				return exitCodeLoadConfigFileFailed
			}
		}

		_ = viper.Unmarshal(&app.Configuration)
	}

	if app.Connections == 0 {
		println("fatal: zero connections")
		return exitCodeFatal
	}

	if app.Parallel == 0 {
		app.Parallel = app.Connections
	}

	if app.Connections < app.Parallel {
		app.Connections = app.Parallel
	}

	var limitRate int64

	if app.LimitRate != "" {
		n, ok := parseBytes(app.LimitRate)
		if !ok {
			println("fatal: invalid limit-rate:", app.LimitRate)
			return exitCodeFatal
		}
		if n > 0 {
			burst := int(n) + int(n>>9)
			app.rateLimiter = rate.NewLimiter(rate.Limit(n), burst)
		}
		limitRate = n
	}

	if app.MinDesiredRate != "" {
		n, ok := parseBytes(app.MinDesiredRate)
		if !ok {
			println("fatal: invalid min-desired-rate:", app.MinDesiredRate)
			return exitCodeFatal
		}
		if n > limitRate && limitRate > 0 {
			println("fatal: min-desired-rate must not be greater than limit-rate")
			return exitCodeFatal
		}
		if n > 0 {
			app.minDesiredRate = int64(float64(n) * (float64(updateInterval) / float64(time.Second)))
		}
	}

	if app.Timeout > 0 {
		timeoutFlagProvided := cmd.Flags().Changed("timeout")
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

	if app.showConfig {
		enc := yaml.NewEncoder(os.Stdout)
		_ = enc.Encode(&app.Configuration)
		enc.Close()

		return exitCodeOK
	}

	cookieJar, err := cookiejar.New(&cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	})
	if err != nil {
		println("fatal:", err)
		return exitCodeFatal
	}

	if !app.showStatus {
		if app.URL == "" {
			if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) != 0 {
				print("Enter url: ")
			}

			stdin := bufio.NewScanner(os.Stdin)
			if !stdin.Scan() {
				return exitCodeURLDeficient
			}

			app.URL = strings.TrimSpace(stdin.Text())
			if app.URL == "" {
				return exitCodeURLDeficient
			}
		}

		if u, err := url.Parse(app.URL); err != nil {
			println("fatal:", err)
			return exitCodeFatal
		} else if u.Scheme != "http" && u.Scheme != "https" {
			println("fatal: unsupported scheme:", u.Scheme)
			return exitCodeFatal
		}

		if app.CookieFile != "" {
			cookiefile := os.ExpandEnv(app.CookieFile)

			err := loadCookies(cookiefile, cookieJar)
			if err != nil && !os.IsNotExist(err) {
				println(err)
				return exitCodeLoadCookieFileFailed
			}
		}
	}

	filename := os.ExpandEnv(app.OutputFile)
	if filename == "" {
		filename = guestFilename(app.URL)
		if filename == "" {
			filename = sanitizeFilename(app.URL)
		}
	}

	fileexists := false

	switch _, err := os.Stat(filename); {
	case err == nil:
		fileexists = true
	case !os.IsNotExist(err) || app.showStatus || app.verifyOnly:
		println(err)
		return exitCodeOpenDataFileFailed
	}

	app.file, err = openDataFile(filename)
	if err != nil {
		println(err)
		return exitCodeOpenDataFileFailed
	}

	defer func() {
		err := app.file.Close()
		if err != nil {
			println(err)
		}

		completeSize := app.file.CompleteSize()
		if completeSize == 0 && !fileexists {
			os.Remove(filename)
			os.Remove(app.file.HashFile())
		}
	}()

	if fileexists {
		if err := app.file.LoadHashFile(); err != nil {
			if os.IsNotExist(err) {
				filename := filename
				if !filepath.IsAbs(filename) {
					filename = filepath.Join(app.workdir, filename)
				}

				println("file exists:", filename)

				return exitCodeOutputFileExists
			}

			println(err)

			return exitCodeLoadHashFileFailed
		}
	}

	if app.showStatus {
		app.status(os.Stdout)
		return exitCodeOK
	}

	if app.Proxy != "" {
		os.Unsetenv("http_proxy")
		os.Unsetenv("HTTP_PROXY")
		os.Unsetenv("https_proxy")
		os.Unsetenv("HTTPS_PROXY")

		if strings.ToUpper(app.Proxy) != "DIRECT" {
			os.Setenv("HTTP_PROXY", app.Proxy)
			os.Setenv("HTTPS_PROXY", app.Proxy)
		}
	}

	if app.Range != "" {
		var s intervals.Set[elems.Int64]

		for _, r0 := range strings.Split(app.Range, ",") {
			r := strings.Split(r0, "-")
			if len(r) > 2 {
				println("fatal: invalid range:", r0)
				return exitCodeFatal
			}

			i, err := strconv.ParseInt(r[0], 10, 32)
			if err != nil {
				println("fatal: invalid range:", r0)
				return exitCodeFatal
			}

			if len(r) == 1 {
				s = intervals.Add(s, intervals.Range(elems.Int64(i*1024*1024), elems.Int64((i+1)*1024*1024)))
				continue
			}

			if r[1] == "" {
				s = intervals.Add(s, intervals.Range(elems.Int64(i*1024*1024), math.MaxInt64))
				continue
			}

			j, err := strconv.ParseInt(r[1], 10, 32)
			if err != nil || j < i {
				println("fatal: invalid range:", r0)
				return exitCodeFatal
			}

			s = intervals.Add(s, intervals.Range(elems.Int64(i*1024*1024), elems.Int64((j+1)*1024*1024)))
		}

		if len(s) > 0 {
			app.file.SetRange(s)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	done := ctx.Done()
	defer stop()

	if app.verifyOnly {
		return app.verify(ctx)
	}

	var (
		streamDone chan struct{}
		streamRest chan struct{}
	)

	if app.streamToStdout {
		streamDone = make(chan struct{})
		streamRest = make(chan struct{})

		defer func() {
			select {
			case <-streamRest:
			default:
				close(streamRest)
			}
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
				case <-done:
					return
				case <-streamTicker.C:
					n, err := app.file.ReadAt(streamBuffer, streamOffset)
					if n > 0 {
						streamOffset += int64(n)
						app.streamOffset.Store(streamOffset)

						if _, err := os.Stdout.Write(streamBuffer[:n]); err != nil {
							stop() // Exiting.
							return
						}
					}
					if err == io.EOF {
						return
					}
				case <-streamRest:
					contentSize := app.file.ContentSize()
					for streamOffset < contentSize {
						n, err := app.file.ReadAt(streamBuffer, streamOffset)
						if n > 0 {
							streamOffset += int64(n)
							if _, err := os.Stdout.Write(streamBuffer[:n]); err != nil {
								return
							}
						}
						if err != nil {
							return
						}
						select {
						case <-done:
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
			println("fatal:", err)
			return exitCodeFatal
		}

		exitHandler := http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				stop()
			},
		)
		http.Handle("/exit", exitHandler)
		http.Handle("/quit", exitHandler)

		http.Handle("/status", http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				app.status(w)
			},
		))

		http.Handle("/stream", http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if app.file.ContentSize() == 0 {
					http.Error(w, "download not started", http.StatusServiceUnavailable)
					return
				}

				f := func(p []byte, off int64) (n int, err error) {
					n, err = app.file.ReadAt(p, off)

					for err == ErrIncomplete {
						select {
						case <-done:
							return
						case <-time.After(time.Second):
							n, err = app.file.ReadAt(p, off)
						}
					}

					return
				}
				content := io.NewSectionReader(ReaderAtFunc(f), 0, app.file.ContentSize())
				http.ServeContent(w, r, filepath.Base(filename), time.Time{}, content)
			},
		))

		srv := &http.Server{}

		defer func() {
			timeout := make(chan bool, 1)
			timeout <- true

			time.AfterFunc(updateInterval, func() {
				if <-timeout {
					println("shuting down remote control server...")
					close(timeout)
				}
			})

			_ = srv.Shutdown(ctx)

			if <-timeout {
				close(timeout)
			}
		}()

		go func() { _ = srv.Serve(l) }()
	}

	canRequest := make(chan struct{})
	app.canRequest = canRequest

	go func() {
		var (
			timer  *time.Timer
			timerC <-chan time.Time
		)

		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()

		cr := canRequest

		for {
			select {
			case <-done:
				return
			case <-timerC:
				cr = canRequest
			case cr <- struct{}{}:
				cr = nil
				if timer == nil {
					timer = time.NewTimer(app.Interval)
					timerC = timer.C
				} else {
					timer.Reset(app.Interval)
				}
			}
		}
	}()

	dialer := &net.Dialer{
		Timeout:   app.Timeout,
		KeepAlive: app.KeepAlive,
		DualStack: true,
	}

	app.client = &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext, // Disables HTTP/2.
			DisableKeepAlives:     app.DisableKeepAlives,
			ForceAttemptHTTP2:     false, // Do not attempt HTTP/2, we want parallel connections.
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   app.TLSHandshakeTimeout,
			ResponseHeaderTimeout: app.ResponseHeaderTimeout,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Jar: cookieJar,
	}

	app.dl.Autorun(func() {
		app.dl.Add(1)
		go func() {
			defer app.dl.Done()
			app.dl.Run()
		}()
	})

	app.fws.Autorun(func() {
		app.fws.Add(1)
		go func() {
			defer app.fws.Done()
			app.fws.Run()
		}()
	})

	app.buf.New = func() any { return new(readWriteBuffer) }
	app.buf.sem = make(chan struct{}, min(max(app.Connections*8, 32), 256))

	for i := uint(0); i <= app.retryCount || app.retryForever; i++ {
		contentSize := app.file.ContentSize()
		completeSize := app.file.CompleteSize()

		if contentSize == 0 || completeSize != contentSize {
			if exitCode := app.download(ctx); exitCode != exitCodeOK {
				if exitCode == exitCodeIncomplete {
					continue
				}

				return exitCode
			}

			contentSize = app.file.ContentSize()
			completeSize = app.file.CompleteSize()

			if completeSize < contentSize {
				return exitCodeOK // Successfully downloaded specified range.
			}
		}

		if !app.Verify {
			return exitCodeOK
		}

		return app.verify(ctx)
	}

	return exitCodeIncomplete
}

func (app *App) download(ctx context.Context) int {
	var (
		firstRecvTime    time.Time
		totalRecv        int64
		requestPermitted async.State[bool]
		numRequest       async.State[uint]
		numResponse      async.State[uint]
		errorCount       async.State[uint]
		exitCode         async.State[int]
		waitStreaming    bool
	)

	var alloc allocState

	var syn syncState

	currentURL := app.URL
	maxConnections := app.Connections

	re1 := regexp.MustCompile(`^bytes (\d+)-(\d+)/(\d+)$`)
	re2 := regexp.MustCompile(`^bytes \*/(\d+)$`)
	errConnTimeout := errors.New("connection timeout")
	errReadTimeout := errors.New("read timeout")
	errTryAgain := errors.New("try again")

	isTimeoutError := func(err error) bool {
		return errors.Is(err, errConnTimeout) || errors.Is(err, errReadTimeout)
	}

	const MaxHistory = 5

	type Download struct {
		CreateTime time.Time
		Global     bool
		Increment  struct {
			History []int64
			Current int64
		}
		Rate int64
	}

	newDownload := func(global bool) *Download {
		return &Download{
			CreateTime: time.Now(),
			Global:     global,
		}
	}

	ema := func(a, b int64) int64 {
		return (4*a + b) / 5
	}

	sum := func(s []int64) int64 {
		var tot int64
		for _, v := range s {
			tot += v
		}
		return tot
	}

	avg := func(s []int64) int64 {
		return sum(s) / int64(len(s))
	}

	updateRate := func(d *Download) {
		di := &d.Increment
		newIncrement := di.Current
		di.Current = 0

		if newIncrement == 0 && len(di.History) == 0 {
			return
		}

		if newIncrement == 0 && d.Global && numResponse.Get() == 0 {
			di.History = di.History[:0]
			d.Rate = 0
			return
		}

		if len(di.History) < MaxHistory {
			di.History = append(di.History, newIncrement)
			d.Rate = avg(di.History)
			return
		}

		di.History = slices.Delete(di.History, 0, 1)
		di.History = append(di.History, newIncrement)
		d.Rate = ema(d.Rate, avg(di.History))
	}

	gd := newDownload(true)

	downloads := make(map[*Download]context.CancelCauseFunc)

	var lowRateKill func()

	if app.minDesiredRate > 0 {
		errLowRate := errors.New("low rate")

		aboutToComplete := func() bool {
			rate := float64(gd.Rate) * (float64(time.Second) / float64(updateInterval))
			etaSeconds := float64(app.file.IncompleteSize()) / rate
			return etaSeconds < 30
		}

		n := 0

		lowRateKill = func() {
			ok := numRequest.Get() == 0 &&
				(numResponse.Get() == maxConnections || !app.file.HasIncomplete()) &&
				gd.Rate < app.minDesiredRate && !aboutToComplete()
			if !ok {
				n = 0
				return
			}

			const N = 5
			if n < N {
				n++
			}

			if n == N {
				for d, cancel := range downloads {
					if d.Rate <= gd.Rate && time.Since(d.CreateTime) >= N*updateInterval {
						cancel(errLowRate)
						delete(downloads, d)
					}
				}
			}
		}
	}

	updateCount := 0

	updateStatusLine := func() {
		updateCount++

		contentSize := app.file.ContentSize()
		completeSize := app.file.CompleteSize()
		progress := int(float64(completeSize) / float64(contentSize) * 100)

		b := &app.statusLineBuffer
		b.Reset()
		b.WriteString(time.Now().Format("15:04:05"))
		fmt.Fprint(b, " ", strings.TrimSuffix(formatBytes(completeSize), "i"))
		fmt.Fprint(b, "/", strings.TrimSuffix(formatBytes(contentSize), "i"))
		fmt.Fprint(b, " ", progress, "%")

		haveTrailing := waitStreaming || alloc.InProgress ||
			syn.InProgress && time.Since(syn.StartTime) >= updateInterval

		connections := numRequest.Get() + numResponse.Get()
		canHaveTrailing := connections == 0 || updateCount%2 == 0

		if connections != 0 {
			fmt.Fprint(b, " CN:", numResponse.Get())
			if numRequest.Get() != 0 {
				fmt.Fprintf(b, "(%v)", numRequest.Get())
			}
			if !haveTrailing || !canHaveTrailing {
				rate := int64(float64(gd.Rate) * (float64(time.Second) / float64(updateInterval)))
				if rate != 0 {
					etaSeconds := int64(math.Ceil(float64(app.file.IncompleteSize()) / float64(rate)))
					fmt.Fprint(b, " DL:", strings.TrimSuffix(formatBytes(rate), "i"))
					fmt.Fprint(b, " ETA:", formatETA(time.Duration(etaSeconds)*time.Second))
				}
				canHaveTrailing = rate == 0
			}
		}

		if haveTrailing && canHaveTrailing {
			switch {
			case waitStreaming:
				b.WriteString(" streaming...")
			case alloc.InProgress:
				fmt.Fprintf(b, " allocating...%v%%", alloc.Progress.Load())
			case syn.InProgress && time.Since(syn.StartTime) >= updateInterval:
				b.WriteString(" sync...")
			}
		}

		app.setStatusLine(b.String())
	}

	reportProgress := func() {
		timeUsed := max(time.Since(firstRecvTime), time.Second)
		app.printMessage(fmt.Sprintf(
			"recv %vB in %v, %vB/s, %v%% complete",
			formatBytes(totalRecv),
			formatTimeUsed(timeUsed),
			formatBytes(int64(float64(totalRecv)/timeUsed.Seconds())),
			int(float64(app.file.CompleteSize())/float64(app.file.ContentSize())*100),
		))
	}

	end := make(chan struct{})

	startTicker := func(d time.Duration, p string, t async.Task) {
		app.dl.Add(1)
		go func() {
			defer app.dl.Done()

			tk := time.NewTicker(d)
			defer tk.Stop()

			for {
				select {
				case <-end:
					return
				case <-tk.C:
					app.dl.Spawn(p, t)
				}
			}
		}()
	}

	var startUpdateTimer async.State[bool]

	if app.file.ContentSize() > 0 {
		startUpdateTimer.Set(true)
	}

	app.dl.Spawn("update", func(co *async.Coroutine) async.Result {
		if !startUpdateTimer.Get() {
			return co.Await(&startUpdateTimer)
		}
		startTicker(updateInterval, co.Path(), async.Do(func() {
			for d := range downloads {
				updateRate(d)
			}
			updateRate(gd)
			updateStatusLine()
			if lowRateKill != nil {
				lowRateKill()
			}
		}))
		return co.End()
	})

	onResponse := func(url string) {
		numRequest.Set(numRequest.Get() - 1)
		numResponse.Set(numResponse.Get() + 1)
		errorCount.Set(0)
		currentURL = url // Save the redirected one if redirection happens.
		maxConnections = app.Connections
		if firstRecvTime.IsZero() {
			firstRecvTime = time.Now()
			startUpdateTimer.Set(true)
			startTicker(reportInterval, "report", async.Do(reportProgress))
			startTicker(max(app.SyncPeriod, time.Minute), "sync", func(co *async.Coroutine) async.Result {
				if syn.InProgress {
					return co.End()
				}
				return co.Switch(app.sync(&syn, false, false))
			})
		}
	}

	onProgress := func(d *Download, n int64) {
		totalRecv += n
		d.Increment.Current += n
		gd.Increment.Current += n
	}

	onComplete := func(d *Download, err error, fatal int, responsed bool) {
		delete(downloads, d)

		if !responsed {
			numRequest.Set(numRequest.Get() - 1)
		} else {
			numResponse.Set(numResponse.Get() - 1)
		}

		switch e := err.(type) {
		case *net.OpError:
			err = e.Err
		case *url.Error:
			err = e.Err
		}

		switch {
		case err == nil:
		case err == errTryAgain:
		case errors.Is(err, context.Canceled):
		case responsed && !app.Verbose:
		default:
			if fatal != 0 {
				exitCode.Set(fatal)
			}

			verbose := app.Verbose || fatal != 0

			if !responsed && !app.IgnoreErrors && (!isTimeoutError(err) || app.TimeoutIntolerant) {
				errorCount.Set(errorCount.Get() + 1)
				err = fmt.Errorf("error: %w", err)
				verbose = true
			}

			if verbose {
				app.printMessage(err)
			}
		}
	}

	app.dl.Spawn("main", async.Chain(
		func(co *async.Coroutine) async.Result {
			if numRequest.Get()+numResponse.Get() == 0 && !app.file.HasIncomplete() {
				exitCode.Set(exitCodeOK)
				return co.End()
			}

			co.Watch(&numRequest, &numResponse)

			if exitCode.Get() != 0 {
				if numRequest.Get()+numResponse.Get() == 0 {
					return co.End()
				}
				return co.Await()
			}

			co.Watch(&exitCode)

			if errorCount.Get() != 0 {
				if currentURL == app.URL { // No redirection.
					if numRequest.Get() != 0 {
						return co.Await()
					}
					if numResponse.Get() == 0 { // Failed to start any download.
						exitCode.Set(exitCodeIncomplete)
						return co.End() // Giving up.
					}
					errorCount.Set(0)
					maxConnections = numResponse.Get() // Limit the number of parallel downloads.
				} else {
					errorCount.Set(0)
					currentURL = app.URL
					maxConnections = app.Connections
				}
			}

			co.Watch(&errorCount)

			if !requestPermitted.Get() {
				return co.Await(&requestPermitted)
			}

			goodToGo := numRequest.Get() < app.Parallel &&
				numRequest.Get()+numResponse.Get() < maxConnections &&
				(numRequest.Get() == 0 || app.file.ContentSize() > 0) &&
				ctx.Err() == nil
			if !goodToGo {
				return co.Await()
			}

			offset, size := app.takeIncomplete()
			if size == 0 {
				return co.Await()
			}

			if app.streamToStdout && app.StreamCache > 0 {
				streamCacheSize := int64(app.StreamCache) * 1024 * 1024
				streamOffset := app.streamOffset.Load()
				if offset >= streamOffset+streamCacheSize {
					app.returnIncomplete(offset, size)
					waitStreaming = true
					return co.Await(&requestPermitted) // Recheck periodically.
				}
				waitStreaming = false
			}

			requestPermitted.Set(false)
			numRequest.Set(numRequest.Get() + 1)

			reqCtx, reqCancel := context.WithCancelCause(ctx)

			d := newDownload(false)
			downloads[d] = reqCancel

			app.dl.Add(1)
			go func() {
				defer app.dl.Done()

				var (
					err       error
					fatal     int
					responsed bool
				)

				var fws *sync.WaitGroup

				defer func() {
					if fws != nil {
						fws.Wait()
					}

					if err != nil && errors.Is(err, context.Canceled) {
						if ctxErr := reqCtx.Err(); ctxErr != nil {
							if cause := context.Cause(reqCtx); cause != ctxErr {
								err = cause
							}
						}
					}

					app.returnIncomplete(offset, size)
					reqCancel(nil)

					app.dl.Spawn("complete", async.Do(func() {
						onComplete(d, err, fatal, responsed)
					}))
				}()

				req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, currentURL, nil)
				if err != nil {
					return
				}

				req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+size-1))

				if app.Referer != "" {
					req.Header.Set("Referer", app.Referer)
				}

				req.Header.Set("User-Agent", app.UserAgent)

				reqTimer := time.AfterFunc(app.Timeout, func() { reqCancel(errConnTimeout) })
				defer reqTimer.Stop()

				resp, err := app.client.Do(req)
				if err != nil {
					return
				}

				defer resp.Body.Close()

				if !reqTimer.Stop() {
					err = errTryAgain
					return
				}

				var contentSize int64

				switch resp.StatusCode {
				case http.StatusOK:
					if offset > 0 {
						err = errors.New("fatal: this server does not support partial requests")
						fatal = exitCodeFatal
						return
					}

					contentSize = resp.ContentLength
				case http.StatusPartialContent:
					contentRange := resp.Header.Get("Content-Range")
					if contentRange == "" {
						err = errors.New("fatal: Content-Range not found in response header")
						fatal = exitCodeFatal
						return
					}

					slice := re1.FindStringSubmatch(contentRange)
					if slice == nil {
						err = fmt.Errorf("fatal: Content-Range unrecognized: %v", contentRange)
						fatal = exitCodeFatal
						return
					}

					contentSize, _ = strconv.ParseInt(slice[3], 10, 64)
				case http.StatusRequestedRangeNotSatisfiable:
					contentRange := resp.Header.Get("Content-Range")
					if contentRange != "" && app.file.ContentSize() == 0 {
						slice := re2.FindStringSubmatch(contentRange)
						if slice != nil {
							contentSize, _ = strconv.ParseInt(slice[1], 10, 64)
							if contentSize > 0 {
								app.file.SetContentSize(contentSize)
								err = errTryAgain
								return
							}
						}
					}
					err = errors.New("fatal: " + resp.Status)
					fatal = exitCodeFatal
					return
				default:
					err = errors.New(resp.Status)
					return
				}

				if contentSize <= 0 {
					err = errors.New("fatal: Content-Length unknown or zero")
					fatal = exitCodeFatal
					return
				}

				shouldAlloc := false
				shouldSync := false

				switch app.file.ContentSize() {
				case contentSize:
				case 0:
					app.file.SetContentSize(contentSize)
					shouldAlloc = app.Alloc
					shouldSync = true
				default:
					err = errors.New("fatal: Content-Length mismatched")
					fatal = exitCodeFatal
					return
				}

				eTag := resp.Header.Get("ETag")
				if eTag != "" {
					switch app.file.EntityTag() {
					case eTag:
					case "":
						app.file.SetEntityTag(eTag)
						shouldSync = true
					default:
						if !app.SkipETag {
							err = errors.New("fatal: ETag mismatched")
							fatal = exitCodeFatal
							return
						}
					}
				}

				lastModified := resp.Header.Get("Last-Modified")
				if lastModified != "" {
					switch app.file.LastModified() {
					case lastModified:
					case "":
						app.file.SetLastModified(lastModified)
						shouldSync = true
					default:
						if !app.SkipLastModified {
							err = errors.New("fatal: Last-Modified mismatched")
							fatal = exitCodeFatal
							return
						}
					}
				}

				contentDisposition := resp.Header.Get("Content-Disposition")
				if contentDisposition != "" {
					_, params, _ := mime.ParseMediaType(contentDisposition)
					if filename := params["filename"]; filename != "" {
						if filename != app.file.Filename() {
							app.file.SetFilename(filename)
							shouldSync = true
						}
					}
				}

				responsed = true

				url := resp.Request.URL.String()
				app.dl.Spawn("response", async.Do(func() {
					onResponse(url)
				}))

				if shouldAlloc {
					var allocErr error

					var wg sync.WaitGroup

					wg.Add(1)

					app.dl.Spawn("alloc", async.Chain(
						app.alloc(ctx, &alloc),
						async.Do(func() {
							allocErr = alloc.Error
							wg.Done()
						}),
					))

					wg.Wait()

					err = allocErr
					if err != nil {
						fatal = exitCodeFatal
						return
					}
				}

				if app.Truncate {
					err = app.file.Truncate(contentSize)
					if err != nil {
						err = fmt.Errorf("truncate: %w", err)
						fatal = exitCodeTruncateFailed
						return
					}
				}

				if shouldSync {
					var synErr error

					var wg sync.WaitGroup

					wg.Add(1)

					app.dl.Spawn("sync", async.Chain(
						syn.Await(),
						app.sync(&syn, false, true),
						async.Do(func() {
							synErr = syn.Error
							wg.Done()
						}),
					))

					wg.Wait()

					err = synErr
					if err != nil {
						fatal = exitCodeSyncFailed
						return
					}
				}

				var increment atomic.Int64

				progress := async.Do(func() {
					onProgress(d, increment.Swap(0))
				})

				readTimer := time.AfterFunc(app.ReadTimeout, func() { reqCancel(errReadTimeout) })
				resetTimer := false

				fws = new(sync.WaitGroup)

				for {
					if resetTimer {
						readTimer.Reset(app.ReadTimeout)
					}

					var n int

					n, err = app.readAndWrite(resp.Body, offset, size, fws)

					readTimer.Stop()
					resetTimer = true

					if n > 0 {
						n64 := int64(n)
						offset, size = offset+n64, size-n64

						if increment.Add(n64) == n64 {
							app.dl.Spawn("progress", progress)
						}

						if err == nil && app.rateLimiter != nil {
							err = app.rateLimiter.WaitN(reqCtx, n)
						}
					}

					if err != nil {
						if err == io.EOF {
							err = nil
							return
						}

						return
					}
				}
			}()

			return co.Await()
		},
		async.Do(func() { close(end) }),
	))

	app.dl.Add(1)
	go func() {
		defer app.dl.Done()

		done := ctx.Done()

		for {
			select {
			case <-end:
				return
			case <-done:
				app.dl.Spawn("cancel", async.Do(func() {
					exitCode.Set(exitCodeCanceled)
				}))
				return
			case <-app.canRequest:
				app.dl.Spawn("canrequest", async.Do(func() {
					requestPermitted.Set(true)
				}))
			}
		}
	}()

	app.dl.Wait()

	if app.statusLine != "" && exitCode.Get() != 0 {
		app.statusLine = ""
		println()
	}

	if totalRecv != 0 {
		reportProgress()
	}

	end = make(chan struct{})

	startTicker(updateInterval, "update", async.Do(updateStatusLine))

	app.dl.Spawn("sync", async.Chain(
		app.sync(&syn, true, false),
		async.Do(func() { close(end) }),
	))

	app.dl.Wait()

	if err := syn.Error; err != nil {
		app.printMessage(err)
	}

	if app.statusLine != "" {
		app.statusLine = ""
		println()
	}

	return exitCode.Get()
}

func (app *App) readAndWrite(body io.Reader, offset, size int64, wg *sync.WaitGroup) (n int, err error) {
	if size == 0 {
		return 0, io.EOF
	}

	app.buf.sem <- struct{}{}

	b := app.buf.Get().(*readWriteBuffer)
	n, err = body.Read(b.Data[:min(size, readBufferSize)])

	if n == 0 {
		app.buf.Put(b)
		<-app.buf.sem
		return
	}

	b.N, b.Offset, b.WaitGroup = n, offset, wg

	wg.Add(1)

	if b.Task == nil {
		app, b := app, b
		b.Task = async.Do(func() {
			_, _ = app.file.WriteAt(b.Data[:b.N], b.Offset)
			b.WaitGroup.Done()
			app.buf.Put(b)
			<-app.buf.sem
		})
	}

	app.fws.Spawn("/", b.Task)

	return
}

func (app *App) takeIncomplete() (offset, size int64) {
	splitSize := app.MinSplitSize

	if incompleteSize := app.file.IncompleteSize(); incompleteSize > 0 {
		average := float64(incompleteSize) / float64(app.Connections)
		splitSize = uint(math.Ceil(average / (1024 * 1024)))
		switch {
		case splitSize > app.MaxSplitSize && app.MaxSplitSize > 0:
			splitSize = app.MaxSplitSize
		case splitSize < app.MinSplitSize && app.MinSplitSize > 0:
			splitSize = app.MinSplitSize
		}
	}

	return app.file.TakeIncomplete(int64(splitSize) * 1024 * 1024)
}

func (app *App) returnIncomplete(offset, size int64) {
	app.file.ReturnIncomplete(offset, size)
}

type allocState struct {
	async.Signal
	InProgress bool
	Progress   atomic.Uint32
	Error      error
}

func (s *allocState) Await() async.Task {
	return func(co *async.Coroutine) async.Result {
		if s.InProgress {
			return co.Await(s)
		}
		return co.End()
	}
}

func (app *App) alloc(ctx context.Context, s *allocState) async.Task {
	return func(co *async.Coroutine) async.Result {
		if !s.InProgress {
			s.InProgress = true
			s.Progress.Store(0)

			var wg sync.WaitGroup

			progress := make(chan int64, 1)
			contentSize := app.file.ContentSize()

			wg.Add(1)
			go func() {
				defer wg.Done()

				p := int64(0)

				for v := range progress {
					if v*100 >= (p+1)*contentSize {
						p = v * 100 / contentSize
						s.Progress.Store(uint32(p))
					}
				}
			}()

			app.dl.Add(1)
			go func() {
				defer app.dl.Done()

				err := app.file.Alloc(ctx, progress)

				close(progress)
				wg.Wait()

				if err != nil {
					err = fmt.Errorf("alloc: %w", err)
				}

				app.dl.Spawn("/", async.Do(func() {
					s.InProgress = false
					s.Error = err
					s.Notify()
				}))
			}()
		}

		return co.Switch(s.Await())
	}
}

type syncState struct {
	async.Signal
	InProgress bool
	StartTime  time.Time
	Error      error
}

func (s *syncState) Await() async.Task {
	return func(co *async.Coroutine) async.Result {
		if s.InProgress {
			return co.Await(s)
		}
		return co.End()
	}
}

func (app *App) sync(s *syncState, waitWrite, syncNow bool) async.Task {
	return func(co *async.Coroutine) async.Result {
		if !s.InProgress {
			s.InProgress = true
			s.StartTime = time.Now()

			app.dl.Add(1)
			go func() {
				defer app.dl.Done()

				if waitWrite {
					app.fws.Wait()
				}

				var err error

				if !syncNow {
					err = app.file.Sync()
				} else {
					err = app.file.SyncNow()
				}

				if err != nil {
					err = fmt.Errorf("sync: %w", err)
				}

				app.dl.Spawn("/", async.Do(func() {
					s.InProgress = false
					s.Error = err
					s.Notify()
				}))
			}()
		}

		return co.Switch(s.Await())
	}
}

func (app *App) verify(ctx context.Context) int {
	contentSize := app.file.ContentSize()
	completeSize := app.file.CompleteSize()

	t := time.Now().Add(updateInterval)

	p := int(-1)
	s := int64(0)
	w := func(b []byte) (n int, err error) {
		n = len(b)
		s += int64(n)

		if s*100 >= int64(p+1)*contentSize {
			p = int(s * 100 / contentSize)
			if time.Now().After(t) {
				app.setStatusLine(fmt.Sprintf("verifying...%v%%", p))
				t = t.Add(updateInterval)
			}
		}

		return
	}

	if err := app.file.Verify(ctx, WriterFunc(w)); err != nil {
		app.printMessage(fmt.Sprintf("verify: %v", err))

		if ctx.Err() != nil {
			return exitCodeCanceled
		}

		return exitCodeVerificationFailed
	}

	oldCompleteSize := completeSize
	newCompleteSize := app.file.CompleteSize()

	if oldCompleteSize > newCompleteSize {
		app.printMessage("verify: file corrupted")

		if app.fixCorrupted {
			if err := app.file.SyncNow(); err != nil {
				app.printMessage(fmt.Sprintf("sync: %v", err))
				return exitCodeSyncFailed
			}

			app.printMessage("Successfully marked corrupted parts as not downloaded.")

			return exitCodeOK
		}

		corruptedSize := oldCompleteSize - newCompleteSize
		app.printMessage(fmt.Sprintf(
			"About %v%% (%vB) are corrupted.",
			100*corruptedSize/contentSize,
			formatBytes(corruptedSize),
		))
		app.printMessage(`Consider run "resume verify --fix" to mark corrupted parts as not downloaded.`)

		return exitCodeCorruptionDetected
	}

	if app.statusLine != "" {
		app.printMessage("verify: all good")
	}

	if app.Autoremove && !app.verifyOnly {
		if err := os.Remove(app.file.HashFile()); err != nil {
			app.printMessage(fmt.Sprintf("autoremove: %v", err))
		}
	}

	return exitCodeOK
}

func (app *App) status(writer io.Writer) {
	contentSize := app.file.ContentSize()
	completeSize := app.file.CompleteSize()

	if contentSize > 0 {
		progress := int(float64(completeSize) / float64(contentSize) * 100)
		fmt.Fprintln(writer, "Size:", contentSize)
		fmt.Fprintln(writer, "Completed:", completeSize, fmt.Sprintf("(%v%%)", progress))
	} else {
		fmt.Fprintln(writer, "Size: unknown")
		fmt.Fprintln(writer, "Completed:", completeSize)
	}

	if s := app.file.Incomplete(); len(s) != 0 {
		fmt.Fprintln(writer, "Incomplete(MiB):", formatSet(s))
	}

	if entityTag := app.file.EntityTag(); entityTag != "" {
		fmt.Fprintln(writer, "ETag:", entityTag)
	}

	if lastModified := app.file.LastModified(); lastModified != "" {
		fmt.Fprintln(writer, "Last-Modified:", lastModified)
	}

	if filename := app.file.Filename(); filename != "" {
		fmt.Fprintln(writer, "Filename:", filename)
	}
}

func (app *App) setStatusLine(s string) {
	app.statusLine = s
	print("\033[1K\r")
	print(s)
}

func (app *App) printMessage(v any) {
	app.statusLine = ""
	print("\033[1K\r")
	println(time.Now().Format("15:04:05"), v)
}

func print(a ...any) {
	fmt.Fprint(os.Stderr, a...)
}

func println(a ...any) {
	fmt.Fprintln(os.Stderr, a...)
}

func formatSet(s intervals.Set[elems.Int64]) string {
	var b strings.Builder
	for i, r := range s {
		if i > 0 {
			b.WriteByte(',')
		}
		lo := int(r.Low / (1024 * 1024))
		b.WriteString(strconv.Itoa(lo))
		if r.High == math.MaxInt64 {
			b.WriteByte('-')
			break
		}
		hi := int(math.Ceil(float64(r.High)/(1024*1024))) - 1
		if lo < hi {
			b.WriteByte('-')
			b.WriteString(strconv.Itoa(hi))
		}
	}
	return b.String()
}

func formatBytes(n int64) string {
	if n == 0 {
		return "0"
	}

	if n < 1024 {
		return strconv.FormatInt(n, 10)
	}

	kilobytes := int64(math.Ceil(float64(n) / 1024))
	if kilobytes < 1000 {
		return strconv.FormatInt(kilobytes, 10) + "Ki"
	}

	if kilobytes < 102400 {
		return strconv.FormatFloat(float64(n)/(1024*1024), 'f', 1, 64) + "Mi"
	}

	megabytes := int64(math.Ceil(float64(n) / (1024 * 1024)))

	return strconv.FormatInt(megabytes, 10) + "Mi"
}

func formatTimeUsed(d time.Duration) (s string) {
	switch {
	case d < time.Minute:
		s = d.Round(time.Second).String()
	default:
		s = d.Round(time.Minute).String()
	}

	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}

	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}

	return
}

func formatETA(d time.Duration) (s string) {
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

func parseBytes(s string) (i int64, ok bool) {
	n := 0

	switch s[len(s)-1] {
	case 'K', 'k':
		n = 10
	case 'M', 'm':
		n = 20
	case 'G', 'g':
		n = 30
	}

	if n > 0 {
		s = s[:len(s)-1]
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil || i < 0 {
		return 0, false
	}

	return i << n, true
}

func isDir(name string) bool {
	stat, err := os.Stat(name)
	if err != nil {
		return false
	}

	return stat.IsDir()
}

func guestFilename(rawurl string) (name string) {
	i := strings.LastIndexAny(rawurl, "/\\?#")
	if i != -1 && (rawurl[i] == '/' || rawurl[i] == '\\') {
		name = sanitizeFilename(rawurl[i+1:])
		if s, err := url.PathUnescape(name); err == nil && utf8.ValidString(s) {
			name = s
		}
	}

	return
}

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`_?[<>:"/\\|?*]+_?`)
	return re.ReplaceAllString(name, "_")
}

func loadCookies(name string, jar http.CookieJar) (err error) {
	file, err := os.Open(name)
	if err != nil {
		return
	}
	defer file.Close()

	s := bufio.NewScanner(file)
	lineNumber := 0

	for s.Scan() {
		line := s.Text()
		lineNumber++

		if line == "" || strings.HasPrefix(line, "# ") {
			continue
		}

		fields := strings.Split(line, "\t")
		if len(fields) != 7 {
			return fmt.Errorf(
				"%v(%v): incorrect field number: want 7, but got %v",
				name, lineNumber, len(fields),
			)
		}

		expires, err := strconv.ParseInt(fields[4], 10, 64)
		if err != nil {
			return fmt.Errorf("%v(%v): expires(field #5): not an integer", name, lineNumber)
		}

		cookie := http.Cookie{
			Name:    fields[5],
			Value:   fields[6],
			Path:    fields[2],
			Domain:  fields[0],
			Expires: time.Unix(expires, 0),
			Secure:  strings.EqualFold(fields[3], "TRUE"),
		}

		if n := len("#HttpOnly_"); len(cookie.Domain) > n && strings.EqualFold(cookie.Domain[:n], "#HttpOnly_") {
			cookie.Domain = cookie.Domain[n:]
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

		jar.SetCookies(
			&url.URL{Scheme: scheme, Host: host},
			[]*http.Cookie{&cookie},
		)
	}

	return
}

type readWriteBuffer struct {
	Data      [readBufferSize]byte
	N         int
	Offset    int64
	WaitGroup *sync.WaitGroup
	Task      async.Task
}

const (
	readBufferSize = 32 << 10
	updateInterval = 1500 * time.Millisecond
	reportInterval = 10 * time.Minute
)

const (
	exitCodeOK                   = 0
	exitCodeFatal                = 1
	exitCodeCanceled             = 2
	exitCodeIncomplete           = 3
	exitCodeURLDeficient         = 4
	exitCodeVerificationFailed   = 5
	_                            = 6
	exitCodeCorruptionDetected   = 7
	exitCodeOutputFileExists     = 8
	exitCodeOpenDataFileFailed   = 9
	exitCodeLoadHashFileFailed   = 10
	exitCodeLoadConfigFileFailed = 11
	exitCodeLoadCookieFileFailed = 12
	exitCodeTruncateFailed       = 13
	exitCodeSyncFailed           = 14
)
