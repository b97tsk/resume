package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/b97tsk/intervals"
	"github.com/b97tsk/intervals/elems"
	"github.com/b97tsk/rx"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/net/publicsuffix"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"
)

const (
	readBufferSize = 8192
	updateInterval = 1500 * time.Millisecond
	reportInterval = 10 * time.Minute
)

const (
	exitCodeOK                   = 0
	exitCodeFatal                = 1
	exitCodeCanceled             = 2
	exitCodeIncomplete           = 3
	exitCodeURLDeficient         = 4
	exitCodeChecksumFailed       = 5
	exitCodeChecksumMismatched   = 6
	exitCodeCorruptionDetected   = 7
	exitCodeOutputFileExists     = 8
	exitCodeOpenDataFileFailed   = 9
	exitCodeLoadHashFileFailed   = 10
	exitCodeLoadConfigFileFailed = 11
	exitCodeLoadCookieFileFailed = 12
	exitCodeTruncateFailed       = 13
	exitCodeSyncFailed           = 14
)

var supportedHashMethods = [...]struct {
	Name   string
	Length int
	New    func() hash.Hash
	Get    func(*DataFile) string
	Set    func(*DataFile, string)
}{
	{
		Name:   "MD5",
		Length: md5.Size * 2,
		New:    md5.New,
		Get:    (*DataFile).ContentMD5,
		Set:    (*DataFile).SetContentMD5,
	},
	{
		Name:   "SHA1",
		Length: sha1.Size * 2,
		New:    sha1.New,
		Get:    (*DataFile).ContentSHA1,
		Set:    (*DataFile).SetContentSHA1,
	},
	{
		Name:   "SHA224",
		Length: sha256.Size224 * 2,
		New:    sha256.New224,
		Get:    (*DataFile).ContentSHA224,
		Set:    (*DataFile).SetContentSHA224,
	},
	{
		Name:   "SHA256",
		Length: sha256.Size * 2,
		New:    sha256.New,
		Get:    (*DataFile).ContentSHA256,
		Set:    (*DataFile).SetContentSHA256,
	},
	{
		Name:   "SHA384",
		Length: sha512.Size384 * 2,
		New:    sha512.New384,
		Get:    (*DataFile).ContentSHA384,
		Set:    (*DataFile).SetContentSHA384,
	},
	{
		Name:   "SHA512",
		Length: sha512.Size * 2,
		New:    sha512.New,
		Get:    (*DataFile).ContentSHA512,
		Set:    (*DataFile).SetContentSHA512,
	},
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
	streamOffset   atomic.Int64
	canRequest     chan struct{}
	rateLimiter    *rate.Limiter
	minDesiredRate int64
	retryCount     uint
	retryForever   bool
}

type Configuration struct {
	Alloc                 bool          `mapstructure:"alloc" yaml:"alloc"`
	Autoremove            bool          `mapstructure:"autoremove" yaml:"autoremove"`
	Connections           uint          `mapstructure:"connections" yaml:"connections"`
	CookieFile            string        `mapstructure:"cookie" yaml:"cookie"`
	DisableKeepAlives     bool          `mapstructure:"disable-keep-alives" yaml:"disable-keep-alives"`
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

	file, err := openDataFile(filename)
	if err != nil {
		println(err)
		return exitCodeOpenDataFileFailed
	}

	defer func() {
		err := file.Close()
		if err != nil {
			println(err)
		}

		completeSize := file.CompleteSize()
		if completeSize == 0 && !fileexists {
			os.Remove(filename)
			os.Remove(file.HashFile())
		}
	}()

	if fileexists {
		if err := file.LoadHashFile(); err != nil {
			if os.IsNotExist(err) {
				filename := filename
				if !filepath.IsAbs(filename) {
					filename = filepath.Join(app.workdir, filename)
				}

				println("file already exists:", filename)

				return exitCodeOutputFileExists
			}

			println(err)

			return exitCodeLoadHashFileFailed
		}
	}

	if app.showStatus {
		app.status(file, os.Stdout)
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
			file.SetRange(s)
		}
	}

	mainCtx, mainCancel := rx.NewBackgroundContext().WithCancel()
	defer mainCancel()

	mainDone := mainCtx.Done()

	interrupt := make(chan os.Signal, 1)
	defer signal.Stop(interrupt)

	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-interrupt:
			mainCancel()
		case <-mainDone:
		}
	}()

	if app.verifyOnly {
		return app.verify(mainCtx, file)
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
				case <-mainDone:
					return
				case <-streamTicker.C:
					if n, err := file.ReadAt(streamBuffer, streamOffset); err == nil {
						streamOffset += int64(n)
						app.streamOffset.Store(streamOffset)

						if _, err := os.Stdout.Write(streamBuffer[:n]); err != nil {
							mainCancel() // Exiting.
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
			println("fatal:", err)
			return exitCodeFatal
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

		streamCounter := rx.MulticastBuffer[int](1)
		streamCounter.Next(0)

		streamNotifier := rx.Unicast[int]()

		rx.Pipe1(
			streamNotifier.Observable,
			rx.Scan(0, func(a, b int) int { return a + b }),
		).Subscribe(
			rx.NewBackgroundContext(),
			streamCounter.Observer,
		)

		http.Handle("/stream", http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if file.ContentSize() == 0 {
					http.Error(w, "download not started", http.StatusServiceUnavailable)
					return
				}

				streamNotifier.Next(1)
				defer streamNotifier.Next(-1)

				done := r.Context().Done()
				f := func(p []byte, off int64) (n int, err error) {
					n, err = file.ReadAt(p, off)

					for err == ErrIncomplete {
						select {
						case <-done:
							return
						case <-mainDone:
							return
						case <-time.After(time.Second):
							n, err = file.ReadAt(p, off)
						}
					}

					return
				}
				content := io.NewSectionReader(ReaderAtFunc(f), 0, file.ContentSize())
				http.ServeContent(w, r, filepath.Base(filename), time.Time{}, content)
			},
		))

		srv := &http.Server{}

		defer func() {
			if streamCounter.BlockingFirst(mainCtx).Value > 0 {
				// Wait until `streamCounter` remains zero for N seconds.
				const N = 5

				println("waiting for remote streaming to complete...")

				_ = rx.Pipe3(
					streamCounter.Observable,
					rx.Filter(func(v int) bool { return v == 0 }),
					rx.SwitchMapTo[int](
						rx.Pipe2(
							rx.Race(
								rx.Pipe1(
									rx.Timer(N*time.Second),
									rx.MapTo[time.Time](-1),
								),
								rx.Pipe2(
									streamCounter.Observable,
									rx.Enumerate[int](0),
									rx.FilterMap(
										func(p rx.Pair[int, int]) (int, bool) {
											// Exclude the first value that is zero.
											if p.Key == 0 && p.Value == 0 {
												return 0, false
											}

											return p.Value, true
										},
									),
								),
							),
							rx.Take[int](1),
							rx.Filter(
								func(v int) bool {
									// Check if this is the value from `rx.Timer`.
									return v == -1
								},
							),
						),
					),
					rx.Take[int](1),
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

			_ = srv.Shutdown(mainCtx.Context)

			if <-timeout {
				close(timeout)
			}
		}()

		go func() { _ = srv.Serve(l) }()
	}

	app.canRequest = make(chan struct{})

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

		canRequest := app.canRequest

		for {
			select {
			case <-mainDone:
				return
			case <-timerC:
				canRequest = app.canRequest
			case canRequest <- struct{}{}:
				canRequest = nil

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

	client := &http.Client{
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

	for i := uint(0); i <= app.retryCount || app.retryForever; i++ {
		contentSize := file.ContentSize()
		completeSize := file.CompleteSize()

		if contentSize == 0 || completeSize != contentSize {
			if exitCode := app.dl(mainCtx, file, client); exitCode != exitCodeOK {
				if exitCode == exitCodeIncomplete {
					continue
				}

				return exitCode
			}

			contentSize = file.ContentSize()
			completeSize = file.CompleteSize()

			if completeSize < contentSize {
				return exitCodeOK // Successfully downloaded specified range.
			}
		}

		if app.streamToStdout {
			select {
			case <-streamRest:
			default:
				close(streamRest)
				select {
				case <-mainDone:
					return exitCodeCanceled
				case <-streamDone:
				}
			}
		}

		if !app.Verify {
			return exitCodeOK
		}

		return app.verify(mainCtx, file)
	}

	return exitCodeIncomplete
}

func (app *App) dl(mainCtx rx.Context, file *DataFile, client *http.Client) int {
	type (
		UpdateTimer struct{}
		ReportTimer struct{}
		SyncTimer   struct{}

		CancelMessage     struct{}
		CanRequestMessage struct{}
		QuitMessage       struct{}

		ResponseMessage struct {
			Tag any
			URL string
		}
		ProgressMessage struct {
			Tag any
			N   int
		}
		CompleteMessage struct {
			Tag       any
			Err       error
			Fatal     int
			Responsed bool
		}

		UpdateMessage struct {
			Connections     int
			DownloadRate    int64
			OngoingRequests int
			WaitStreaming   bool
		}
		ReportMessage struct {
			TimeUsed  time.Duration
			TotalRecv int64
		}
	)

	var writes rx.Observer[func()]

	{
		ctx := rx.NewBackgroundContext().WithNewWaitGroup()

		defer func() {
			writes.Complete()
			ctx.Wait()
			_ = file.Sync()
		}()

		rx.Pipe1(
			rx.NewObservable(
				func(c rx.Context, o rx.Observer[func()]) {
					writes = o // OnBackpressureCongest is safe for concurrent use.
				},
			),
			rx.OnBackpressureCongest[func()](int(app.Connections*4)),
		).Subscribe(ctx, func(n rx.Notification[func()]) {
			if n.Kind == rx.KindNext {
				n.Value()
			}
		})
	}

	type Buffer [readBufferSize]byte

	bufferPool := sync.Pool{
		New: func() any { return new(Buffer) },
	}

	readAndWrite := func(body io.Reader, offset, size int64) (n int, err error) {
		if size == 0 {
			return 0, io.EOF
		}

		if size > readBufferSize {
			size = readBufferSize
		}

		b := bufferPool.Get().(*Buffer)
		n, err = body.Read((*b)[:size])
		if n > 0 {
			writes.Next(func() {
				_, _ = file.WriteAt((*b)[:n], offset)
				bufferPool.Put(b)
			})
		} else {
			bufferPool.Put(b)
		}

		return
	}

	waitWriteDone := func() {
		q := make(chan struct{})
		writes.Next(func() { close(q) })
		<-q
	}

	takeIncomplete := func() (offset, size int64) {
		splitSize := app.MinSplitSize

		if incompleteSize := file.IncompleteSize(); incompleteSize > 0 {
			average := float64(incompleteSize) / float64(app.Connections)
			splitSize = uint(math.Ceil(average / (1024 * 1024)))
			switch {
			case splitSize > app.MaxSplitSize && app.MaxSplitSize > 0:
				splitSize = app.MaxSplitSize
			case splitSize < app.MinSplitSize && app.MinSplitSize > 0:
				splitSize = app.MinSplitSize
			}
		}

		return file.TakeIncomplete(int64(splitSize) * 1024 * 1024)
	}

	returnIncomplete := func(offset, size int64) {
		file.ReturnIncomplete(offset, size)
	}

	ctx, cancel := rx.NewBackgroundContext().WithNewWaitGroup().WithCancel()

	onDownloadStarted := rx.Multicast[any]()
	onUpdateTimer := rx.Multicast[struct{}]()

	defer func() {
		onDownloadStarted.Unsubscribe()
		onUpdateTimer.Unsubscribe()
		cancel()
		ctx.Wait()
	}()

	messages := rx.Unicast[any]()

	startUpdateTimer := onDownloadStarted.Observable
	if file.ContentSize() > 0 {
		startUpdateTimer = rx.Empty[any]()
	}

	ob := rx.Pipe2(
		messages.Observable,
		rx.OnBackpressureCongest[any](int(app.Connections*4)),
		rx.MergeWith(
			rx.Concat(
				startUpdateTimer,
				rx.Pipe1(
					rx.Ticker(updateInterval),
					rx.MapTo[time.Time, any](UpdateTimer{}),
				),
			),
			rx.Concat(
				onDownloadStarted.Observable,
				rx.Pipe1(
					rx.Ticker(reportInterval),
					rx.MapTo[time.Time, any](ReportTimer{}),
				),
			),
			rx.Pipe1(
				rx.Ticker(max(app.SyncPeriod, time.Minute)),
				rx.MapTo[time.Time, any](SyncTimer{}),
			),
			rx.Pipe1(
				rx.NewObservable(
					func(c rx.Context, o rx.Observer[any]) {
						done := c.Done()
						mainDone := mainCtx.Done()
						for {
							select {
							case <-done:
								o.Complete()
								return
							case <-mainDone:
								o.Next(CancelMessage{})
								o.Complete()
								return
							case <-app.canRequest:
								o.Next(CanRequestMessage{})
							}
						}
					},
				),
				rx.Go[any](),
			),
		),
	)

	{
		type DownloadInfo struct {
			DownloadRate int64 // bytes per updateInterval
			RecentRecv   int64
			UpdateCount  int
		}

		var (
			connections    uint
			maxConnections = app.Connections
		)

		newDownloadInfo := func(ctx rx.Context, global bool) *DownloadInfo {
			di := &DownloadInfo{}

			rx.Pipe3(
				onUpdateTimer.Observable,
				rx.Map(
					func(struct{}) int64 {
						v := di.RecentRecv
						di.RecentRecv = 0
						di.UpdateCount++
						return v
					},
				),
				rx.Connect(
					func(source rx.Observable[int64]) rx.Observable[int64] {
						const N = 5

						if global {
							source = rx.Pipe2(
								source,
								rx.SkipWhile(func(v int64) bool { return v == 0 }),
								rx.TakeWhile(func(v int64) bool { return v > 0 || connections > 0 }),
							)
						}

						ob := rx.Pipe2(
							rx.Empty[int64](),
							rx.MergeWith(
								rx.Pipe4(
									source,
									rx.Scan(0, func(a, b int64) int64 { return a + b }),
									rx.Enumerate[int64](1),
									rx.Map(
										func(p rx.Pair[int, int64]) int64 {
											// For the first `N * updateInterval`, we average the speed.
											return p.Value / int64(p.Key)
										},
									),
									rx.Take[int64](N),
								),
								rx.Pipe3(
									source,
									rx.BufferCount[int64](N).WithStartBufferEvery(1),
									rx.Map(
										func(s []int64) int64 {
											sum := int64(0)
											for _, v := range s {
												sum += v
											}
											return sum / int64(len(s))
										},
									),
									rx.Scan(-1, func(a, b int64) int64 {
										if a < 0 {
											return b
										}
										// After `N * updateInterval`, we calculate the speed by using
										// exponential moving average (EMA).
										const ia = 5 // Inverse of alpha.
										return (b + (ia-1)*a) / ia
									}),
								),
							),
							rx.EndWith[int64](0),
						)

						if global {
							ob = rx.Pipe1(ob, rx.RepeatForever[int64]())
						}

						return ob
					},
				),
				rx.DoOnNext(
					func(v int64) {
						di.DownloadRate = v
					},
				),
			).Subscribe(ctx, rx.Noop[int64])

			return di
		}

		gdi := newDownloadInfo(ctx, true)
		tags := make(map[any]rx.CancelCauseFunc)

		var postUpdateTimer rx.Observer[struct{}]

		var ongoingRequests uint

		if app.minDesiredRate > 0 {
			const N = 5

			errLowRate := errors.New("low rate")

			aboutToComplete := func() bool {
				downloadRate := float64(gdi.DownloadRate) * (float64(time.Second) / float64(updateInterval))
				etaSeconds := float64(file.IncompleteSize()) / downloadRate
				return etaSeconds < 30
			}

			cond := func(struct{}) bool {
				return ongoingRequests == 0 &&
					(connections == maxConnections || !file.HasIncomplete()) &&
					gdi.DownloadRate < app.minDesiredRate && !aboutToComplete()
			}

			m := rx.Multicast[struct{}]()
			defer m.Unsubscribe()

			rx.Pipe7(
				m.Observable,
				rx.SkipWhile(func(v struct{}) bool { return !cond(v) }),
				rx.TakeWhile(cond),
				rx.Take[struct{}](N),
				rx.Scan(0, func(a int, _ struct{}) int { return a + 1 }),
				rx.Filter(func(v int) bool { return v == N }),
				rx.DoOnNext(
					func(int) {
						for tag, cancel := range tags {
							di := tag.(*DownloadInfo)
							if di.UpdateCount >= N && di.DownloadRate <= gdi.DownloadRate {
								cancel(errLowRate)
							}
						}
					},
				),
				rx.RepeatForever[int](),
			).Subscribe(ctx, rx.Noop[int])

			postUpdateTimer = m.Observer
		}

		var (
			firstRecvTime    time.Time
			totalRecv        int64
			requestPermitted bool
			waitStreaming    bool
			errorCount       uint
			fatalCode        int
		)

		currentURL := app.URL

		re1 := regexp.MustCompile(`^bytes (\d+)-(\d+)/(\d+)$`)
		re2 := regexp.MustCompile(`^bytes \*/(\d+)$`)
		errConnTimeout := errors.New("connection timeout")
		errReadTimeout := errors.New("read timeout")
		errTryAgain := errors.New("try again")

		report := func() rx.Observable[any] {
			return rx.Just[any](ReportMessage{
				TimeUsed:  max(time.Since(firstRecvTime), time.Second),
				TotalRecv: totalRecv,
			})
		}

		var canQuit, sentQuitMessage bool

		quit := func(code int) rx.Observable[any] {
			if !sentQuitMessage {
				messages.Next(QuitMessage{})
				messages.Complete()
				sentQuitMessage = true
			}

			if !canQuit {
				return rx.Empty[any]()
			}

			ob := rx.Throw[any](exitCodeError(code))
			if totalRecv > 0 {
				ob = rx.Concat(report(), ob)
			}

			return ob
		}

		ob = rx.Pipe1(
			ob,
			rx.MergeMap(
				func(v any) rx.Observable[any] {
					switch w := v.(type) {
					case ProgressMessage:
						firstRecv := totalRecv == 0
						totalRecv += int64(w.N)
						if firstRecv {
							firstRecvTime = time.Now()
							onDownloadStarted.Complete()
						}
						gdi.RecentRecv += int64(w.N)
						w.Tag.(*DownloadInfo).RecentRecv += int64(w.N)
						return rx.Empty[any]()
					case UpdateTimer:
						onUpdateTimer.Next(struct{}{})
						if postUpdateTimer != nil {
							postUpdateTimer.Next(struct{}{})
						}
						return rx.Just[any](UpdateMessage{
							Connections:     int(connections),
							DownloadRate:    int64(float64(gdi.DownloadRate) * (float64(time.Second) / float64(updateInterval))),
							OngoingRequests: int(ongoingRequests),
							WaitStreaming:   waitStreaming,
						})
					case ReportTimer:
						return report()
					case SyncTimer:
						if err := file.Sync(); err != nil {
							return rx.Just[any](fmt.Sprintf("sync failed: %v", err))
						}
						return rx.Empty[any]()
					case CancelMessage:
						fatalCode = exitCodeCanceled
					case CanRequestMessage:
						requestPermitted = true
					case QuitMessage:
						canQuit = true
					case ResponseMessage:
						connections++
						ongoingRequests--
						errorCount = 0
						currentURL = w.URL // Save the redirected one if redirection happens.
						maxConnections = app.Connections
					case CompleteMessage:
						delete(tags, w.Tag)
						if w.Responsed {
							connections--
						} else {
							ongoingRequests--
						}
						switch {
						case w.Err == nil:
						case w.Err == errTryAgain:
						case errors.Is(w.Err, context.Canceled):
						case w.Responsed && !app.Verbose:
						default:
							if w.Fatal != 0 {
								fatalCode = w.Fatal
							}
							verbose := app.Verbose || w.Fatal != 0
							if w.Err != errConnTimeout && w.Err != errReadTimeout || app.TimeoutIntolerant {
								errorCount++
								if connections == 0 {
									verbose = true
								}
							}
							if verbose {
								err := w.Err
								switch e := err.(type) {
								case *net.OpError:
									err = e.Err
								case *url.Error:
									err = e.Err
								}
								messages.Next(err)
							}
						}
					default:
						return rx.Just(v)
					}

					if connections == 0 && ongoingRequests == 0 && !file.HasIncomplete() {
						return quit(exitCodeOK)
					}

					if fatalCode != 0 {
						if connections == 0 && ongoingRequests == 0 {
							return quit(fatalCode)
						}
						return rx.Empty[any]()
					}

					if errorCount != 0 {
						if currentURL == app.URL { // No redirection.
							if ongoingRequests != 0 {
								return rx.Empty[any]()
							}
							if connections == 0 { // Failed to start any download.
								return quit(exitCodeIncomplete) // Giving up.
							}
							errorCount = 0
							maxConnections = connections // Limit the number of parallel downloads.
							return rx.Empty[any]()
						}
						errorCount = 0
						currentURL = app.URL
						maxConnections = app.Connections
					}

					{
						requestPermitted := requestPermitted &&
							ongoingRequests < app.Parallel &&
							connections+ongoingRequests < maxConnections &&
							(ongoingRequests == 0 || file.ContentSize() > 0) &&
							mainCtx.Context.Err() == nil
						if !requestPermitted {
							return rx.Empty[any]()
						}
					}

					offset, size := takeIncomplete()
					if size == 0 {
						return rx.Empty[any]()
					}

					if app.streamToStdout && app.StreamCache > 0 {
						streamCacheSize := int64(app.StreamCache) * 1024 * 1024
						streamOffset := app.streamOffset.Load()
						if offset >= streamOffset+streamCacheSize {
							returnIncomplete(offset, size)
							waitStreaming = true
							return rx.Empty[any]()
						}
						waitStreaming = false
					}

					requestPermitted = false
					ongoingRequests++

					reqCtx, reqCancel := mainCtx.WithCancelCause()

					var tag any = newDownloadInfo(reqCtx, false)

					tags[tag] = reqCancel

					return rx.Pipe1(
						rx.NewObservable(
							func(c rx.Context, o rx.Observer[any]) {
								var (
									err       error
									fatal     int
									responsed bool
								)

								defer func() {
									if responsed {
										waitWriteDone()
									}

									if err != nil && errors.Is(err, context.Canceled) {
										if ctxErr := reqCtx.Context.Err(); ctxErr != nil {
											if cause := reqCtx.Cause(); cause != ctxErr {
												err = cause
											}
										}
									}

									returnIncomplete(offset, size)
									messages.Next(CompleteMessage{tag, err, fatal, responsed})
									o.Complete()
									reqCancel(nil)
								}()

								req, err := http.NewRequestWithContext(reqCtx.Context, http.MethodGet, currentURL, nil)
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

								resp, err := client.Do(req)
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
									if contentRange != "" && file.ContentSize() == 0 {
										slice := re2.FindStringSubmatch(contentRange)
										if slice != nil {
											contentSize, _ = strconv.ParseInt(slice[1], 10, 64)
											if contentSize > 0 {
												file.SetContentSize(contentSize)
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

								switch file.ContentSize() {
								case contentSize:
								case 0:
									file.SetContentSize(contentSize)
									shouldAlloc = app.Alloc
									shouldSync = true
								default:
									err = errors.New("fatal: Content-Length mismatched")
									fatal = exitCodeFatal
									return
								}

								for _, h := range supportedHashMethods {
									hashCode := resp.Header.Get("Content-" + h.Name)
									if hashCode == "" {
										hashCode = resp.Header.Get("X-Checksum-" + h.Name)
										if hashCode == "" {
											switch h.Name {
											case "SHA224", "SHA256", "SHA384", "SHA512":
												hashCode = resp.Header.Get("Content-Sha2")
												if hashCode == "" {
													hashCode = resp.Header.Get("X-Checksum-Sha2")
												}
											}
										}
									}
									if len(hashCode) == h.Length {
										hashCode = strings.ToLower(hashCode)
										switch h.Get(file) {
										case hashCode:
										case "":
											h.Set(file, hashCode)
											shouldSync = true
										default:
											err = errors.New("fatal: " + h.Name + " mismatched")
											fatal = exitCodeFatal
											return
										}
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
											err = errors.New("fatal: ETag mismatched")
											fatal = exitCodeFatal
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
										if filename != file.Filename() {
											file.SetFilename(filename)
											shouldSync = true
										}
									}
								}

								if shouldAlloc {
									err = app.alloc(mainCtx, file)
									if err != nil {
										fatal = exitCodeFatal
										return
									}
								}

								if app.Truncate {
									err = file.Truncate(contentSize)
									if err != nil {
										err = fmt.Errorf("truncate failed: %w", err)
										fatal = exitCodeTruncateFailed
										return
									}
								}

								if shouldSync {
									err = file.SyncNow()
									if err != nil {
										err = fmt.Errorf("sync failed: %w", err)
										fatal = exitCodeSyncFailed
										return
									}
								}

								messages.Next(ResponseMessage{tag, resp.Request.URL.String()})
								responsed = true

								readTimer := time.AfterFunc(app.ReadTimeout, func() { reqCancel(errReadTimeout) })
								resetTimer := false

								for {
									if resetTimer {
										readTimer.Reset(app.ReadTimeout)
									}

									var n int

									n, err = readAndWrite(resp.Body, offset, size)

									readTimer.Stop()
									resetTimer = true

									if n > 0 {
										offset, size = offset+int64(n), size-int64(n)

										messages.Next(ProgressMessage{tag, n})

										if err == nil && app.rateLimiter != nil {
											err = app.rateLimiter.WaitN(reqCtx.Context, n)
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
							},
						),
						rx.Go[any](),
					)
				},
			),
		)
	}

	{
		var statusLine string

		var b bytes.Buffer

		ob = rx.Pipe2(
			ob,
			rx.DoOnNext(
				func(v any) {
					switch w := v.(type) {
					case UpdateMessage:
						contentSize := file.ContentSize()
						completeSize := file.CompleteSize()
						progress := int(float64(completeSize) / float64(contentSize) * 100)

						b.Reset()
						b.WriteString(time.Now().Format("15:04:05"))
						b.WriteString(fmt.Sprint(" ", strings.TrimSuffix(formatBytes(completeSize), "i")))
						b.WriteString(fmt.Sprint("/", strings.TrimSuffix(formatBytes(contentSize), "i")))
						b.WriteString(fmt.Sprint(" ", progress, "%"))

						switch {
						case w.Connections > 0 || w.OngoingRequests > 0:
							b.WriteString(fmt.Sprint(" CN:", w.Connections))
							if w.OngoingRequests > 0 {
								b.WriteString(fmt.Sprintf("(%v)", w.OngoingRequests))
							}
							if w.DownloadRate > 0 {
								etaSeconds := int64(math.Ceil(float64(file.IncompleteSize()) / float64(w.DownloadRate)))
								b.WriteString(fmt.Sprint(" DL:", strings.TrimSuffix(formatBytes(w.DownloadRate), "i")))
								b.WriteString(fmt.Sprint(" ETA:", formatETA(time.Duration(etaSeconds)*time.Second)))
							}
						case w.WaitStreaming:
							b.WriteString(" streaming...")
						}

						if s := b.String(); s != statusLine {
							statusLine = s
							print("\033[1K\r")
							print(s)
						}
					case ReportMessage:
						statusLine = ""
						print("\033[1K\r")
						print(time.Now().Format("15:04:05"))
						printf(
							" recv %vB in %v, %vB/s, %v%% completed\n",
							formatBytes(w.TotalRecv),
							formatTimeUsed(w.TimeUsed),
							formatBytes(int64(float64(w.TotalRecv)/w.TimeUsed.Seconds())),
							int(float64(file.CompleteSize())/float64(file.ContentSize())*100),
						)
					default:
						statusLine = ""
						print("\033[1K\r")
						println(time.Now().Format("15:04:05"), v)
					}
				},
			),
			rx.DoOnTermination[any](
				func() {
					if statusLine != "" {
						println()
					}
				},
			),
		)
	}

	return int(ob.BlockingSubscribe(ctx, rx.Noop[any]).Error.(exitCodeError))
}

type exitCodeError int

func (e exitCodeError) Error() string {
	return fmt.Sprintf("exit code: %v", int(e))
}

func (app *App) verify(mainCtx rx.Context, file *DataFile) int {
	type DigestInfo struct {
		Name string
		Hash hash.Hash
	}

	var digests map[string]DigestInfo

	contentSize := file.ContentSize()
	completeSize := file.CompleteSize()

	if completeSize == contentSize {
		for _, h := range supportedHashMethods {
			hashCode := h.Get(file)
			if hashCode != "" {
				if digests == nil {
					digests = make(map[string]DigestInfo)
				}

				digests[hashCode] = DigestInfo{h.Name, h.New()}
				println(h.Name+":", hashCode)
			}
		}
	}

	var vs rx.Observer[int]

	rx.Pipe2(
		rx.NewObservable(
			func(c rx.Context, o rx.Observer[int]) {
				vs = o
			},
		),
		rx.SkipUntil[int](rx.Timer(time.Second)),
		rx.ThrowIfEmpty[int](),
	).Subscribe(mainCtx, func(n rx.Notification[int]) {
		switch n.Kind {
		case rx.KindNext:
			print("\033[1K\r")
			printf("verifying...%v%%", n.Value)
		case rx.KindComplete:
			print("\033[1K\r")
			println("verifying...DONE")
		case rx.KindError, rx.KindStop:
			if n.Error != rx.ErrEmpty {
				print("\033[1K\r")
				printf("verifying...%v\n", n.Error)
			}
		}
	})

	p := int(-1)
	s := int64(0)
	w := func(b []byte) (n int, err error) {
		n = len(b)
		for _, digest := range digests {
			n, err = digest.Hash.Write(b)
			if err != nil {
				break
			}
		}

		s += int64(n)
		if s*100 >= int64(p+1)*contentSize {
			p = int(s * 100 / contentSize)
			vs.Next(p)
		}

		return
	}

	if err := file.Verify(mainCtx.Context, WriterFunc(w)); err != nil {
		vs.Error(err)

		if mainCtx.Context.Err() != nil {
			return exitCodeCanceled
		}

		return exitCodeChecksumFailed
	}

	oldCompleteSize := completeSize
	newCompleteSize := file.CompleteSize()

	if oldCompleteSize > newCompleteSize {
		vs.Error(errors.New("BAD: file corrupted"))

		if app.fixCorrupted {
			if err := file.SyncNow(); err != nil {
				println("sync failed:", err)
				return exitCodeSyncFailed
			}

			println("Successfully marked corrupted parts as not downloaded.")

			return exitCodeOK
		}

		corruptedSize := oldCompleteSize - newCompleteSize
		printf("About %v%% (%vB) are corrupted.\n", 100*corruptedSize/contentSize, formatBytes(corruptedSize))
		println(`Consider run "resume verify --fix" to mark corrupted parts as not downloaded.`)

		return exitCodeCorruptionDetected
	}

	for hashCode, digest := range digests {
		if hashCode != hex.EncodeToString(digest.Hash.Sum(nil)) {
			vs.Error(fmt.Errorf("BAD: %v mismatched", digest.Name))
			return exitCodeChecksumMismatched
		}
	}

	if app.Autoremove && !app.verifyOnly {
		os.Remove(file.HashFile())
	}

	vs.Complete()

	return exitCodeOK
}

func (app *App) alloc(mainCtx rx.Context, file *DataFile) error {
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

	return file.Alloc(mainCtx.Context, progress)
}

func (app *App) status(file *DataFile, writer io.Writer) {
	contentSize := file.ContentSize()
	completeSize := file.CompleteSize()

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

		j := int(math.Ceil(float64(r.High)/(1024*1024))) - 1
		if i == j {
			items = append(items, strconv.Itoa(i))
			continue
		}

		items = append(items, fmt.Sprintf("%v-%v", i, j))
	}

	if len(items) > 0 {
		fmt.Fprintln(writer, "Incomplete(MiB):", strings.Join(items, ","))
	}

	for _, h := range supportedHashMethods {
		hashCode := h.Get(file)
		if hashCode != "" {
			fmt.Fprintln(writer, h.Name+":", hashCode)
		}
	}

	if entityTag := file.EntityTag(); entityTag != "" {
		fmt.Fprintln(writer, "ETag:", entityTag)
	}

	if lastModified := file.LastModified(); lastModified != "" {
		fmt.Fprintln(writer, "Last-Modified:", lastModified)
	}

	if filename := file.Filename(); filename != "" {
		fmt.Fprintln(writer, "Filename:", filename)
	}
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

func print(a ...any) {
	fmt.Fprint(os.Stderr, a...)
}

func printf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func println(a ...any) {
	fmt.Fprintln(os.Stderr, a...)
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
