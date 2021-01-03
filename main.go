package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
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

	"github.com/b97tsk/rx"
	"github.com/b97tsk/rx/operators"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/net/publicsuffix"
	"gopkg.in/yaml.v2"
)

const (
	readBufferSize          = 4096
	reportInterval          = 10 * time.Minute
	measureInterval         = 1500 * time.Millisecond
	measureIntervalMultiple = float64(measureInterval) / float64(time.Second)
)

var supportedHashMethods = [...]struct {
	Name    string
	IsAlias bool
	Length  int
	New     func() hash.Hash
	Get     func(*DataFile) string
	Set     func(*DataFile, string)
}{
	{
		Name:    "MD5",
		IsAlias: false,
		Length:  md5.Size * 2,
		New:     md5.New,
		Get:     (*DataFile).ContentMD5,
		Set:     (*DataFile).SetContentMD5,
	},
	{
		Name:    "SHA1",
		IsAlias: false,
		Length:  sha1.Size * 2,
		New:     sha1.New,
		Get:     (*DataFile).ContentSHA1,
		Set:     (*DataFile).SetContentSHA1,
	},
	{
		Name:    "SHA2",
		IsAlias: true,
		Length:  sha256.Size * 2,
		New:     sha256.New,
		Get:     (*DataFile).ContentSHA256,
		Set:     (*DataFile).SetContentSHA256,
	},
	{
		Name:    "SHA256",
		IsAlias: false,
		Length:  sha256.Size * 2,
		New:     sha256.New,
		Get:     (*DataFile).ContentSHA256,
		Set:     (*DataFile).SetContentSHA256,
	},
}

type App struct {
	Configure
	workdir        string
	conffile       string
	noUserConfig   bool
	showStatus     bool
	showConfigure  bool
	streamToStdout bool
	streamOffset   *int64
}

type Configure struct {
	Alloc                 bool          `mapstructure:"alloc" yaml:"alloc"`
	Autoremove            bool          `mapstructure:"autoremove" yaml:"autoremove"`
	Connections           uint          `mapstructure:"connections" yaml:"connections"`
	CookieFile            string        `mapstructure:"cookie" yaml:"cookie"`
	DialTimeout           time.Duration `mapstructure:"dial-timeout" yaml:"dial-timeout"`
	Errors                uint          `mapstructure:"errors" yaml:"errors"`
	Interval              time.Duration `mapstructure:"interval" yaml:"interval"`
	KeepAlive             time.Duration `mapstructure:"keep-alive" yaml:"keep-alive"`
	ListenAddress         string        `mapstructure:"listen" yaml:"listen"`
	MaxSplitSize          uint          `mapstructure:"max-split" yaml:"max-split"`
	MinSplitSize          uint          `mapstructure:"min-split" yaml:"min-split"`
	OutputFile            string        `mapstructure:"output" yaml:"output"`
	PerUserAgentLimit     uint          `mapstructure:"per-user-agent-limit" yaml:"per-user-agent-limit"`
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
	Verify                bool          `mapstructure:"verify" yaml:"verify"`
}

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

	must := func(err error) {
		if err != nil {
			println(err)
			os.Exit(1)
		}
	}

	flags := rootCmd.PersistentFlags()

	flags.BoolVar(&app.Alloc, "alloc", false, "alloc disk space before the first write")
	flags.BoolVar(&app.Autoremove, "autoremove", false, "auto remove .resume file after successfully verified")
	flags.BoolVar(&app.TimeoutIntolerant, "timeout-intolerant", false, "treat timeouts as errors")
	flags.BoolVar(&app.Truncate, "truncate", false, "truncate output file before the first write")
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
	flags.DurationVarP(&app.Timeout, "timeout", "t", 0, "if positive, all timeouts default to this value")
	flags.StringVar(&app.CookieFile, "cookie", "", "cookie file")
	flags.StringVarP(&app.ListenAddress, "listen", "L", "", "HTTP listen address for remote control")
	flags.StringVarP(&app.OutputFile, "output", "o", "", "output file")
	flags.StringVarP(&app.Proxy, "proxy", "x", "", "a shorthand for setting http(s)_proxy environment variables")
	flags.StringVarP(&app.Range, "range", "r", "", "request range (MiB), e.g., 0-1023")
	flags.StringVarP(&app.Referer, "referer", "R", "", "referer url")
	flags.StringVarP(&app.UserAgent, "user-agent", "A", "", "user agent")
	flags.UintVarP(&app.Connections, "connections", "c", 4, "maximum number of parallel downloads")
	flags.UintVarP(&app.Errors, "errors", "e", 3, "maximum number of errors")
	flags.UintVarP(&app.MaxSplitSize, "max-split", "s", 0, "maximal split size (MiB), 0 means use maximum possible")
	flags.UintVarP(&app.MinSplitSize, "min-split", "p", 0, "minimal split size (MiB), even smaller value may be used")

	must(viper.BindPFlags(flags))

	flags.BoolVarP(&app.noUserConfig, "no-user-config", "n", false, "do not load .resumerc file")
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
		flags.UintVarP(&app.StreamCache, "cache", "k", 0, "stream cache size (MiB), 0 means unlimited")
		must(viper.BindPFlag("stream-rate", flags.Lookup("rate")))
		must(viper.BindPFlag("stream-cache", flags.Lookup("cache")))
	}
	rootCmd.AddCommand(streamCmd)

	_ = rootCmd.Execute()
}

func (app *App) Main(cmd *cobra.Command, args []string) int {
	if app.workdir != "." {
		err := os.Chdir(app.workdir)
		if err != nil {
			println(err)
			return 1
		}
	}

	os.Setenv("ConfigDir", filepath.Dir(app.conffile))

	if !app.noUserConfig {
		if dir, err := os.UserHomeDir(); err == nil {
			viper.SetConfigFile(filepath.Join(dir, ".resumerc"))
			viper.SetConfigType("yaml")

			if err := viper.ReadInConfig(); err == nil {
				_ = viper.Unmarshal(&app.Configure)
			}
		}
	}

	if app.conffile != "" {
		viper.SetConfigFile(app.conffile)
		viper.SetConfigType("yaml")

		if err := viper.ReadInConfig(); err != nil {
			if cmd.Flags().Changed("conf") || !os.IsNotExist(err) && !isDir(app.conffile) {
				println(err)
				return 1
			}
		}

		_ = viper.Unmarshal(&app.Configure)
	}

	if app.Connections == 0 {
		println("Zero connections are not allowed.")

		if viper.InConfig("connections") && !cmd.Flags().Changed("connections") {
			println("Please check your configure file.")
		}

		return 1
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
		_ = enc.Encode(&app.Configure)
		enc.Close()

		return 0
	}

	cookieJar, err := cookiejar.New(&cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	})
	if err != nil {
		println(err)
		return 1
	}

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

			err := loadCookies(cookiefile, cookieJar)
			if err != nil && !os.IsNotExist(err) {
				println(err)
				return 1
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

				printf("\"%v\" already exists.\n", filename)

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

	if app.Proxy != "" {
		if strings.ToLower(app.Proxy) == "direct" {
			os.Unsetenv("http_proxy")
			os.Unsetenv("HTTP_PROXY")
		} else {
			os.Setenv("http_proxy", app.Proxy)
			os.Setenv("HTTP_PROXY", app.Proxy)
		}

		os.Unsetenv("https_proxy")
		os.Unsetenv("HTTPS_PROXY")
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
	defer mainCancel()

	mainDone := mainCtx.Done()

	go func() {
		interrupt := make(chan os.Signal, 1)
		defer signal.Stop(interrupt)

		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-interrupt:
			mainCancel()
		case <-mainDone:
		}
	}()

	var (
		streamDone chan struct{}
		streamRest chan struct{}
	)

	if app.streamToStdout {
		app.streamOffset = new(int64)
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
						atomic.StoreInt64(app.streamOffset, streamOffset)

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

		streamCounter := rx.MulticastReplay(&rx.ReplayOptions{BufferSize: 1})
		streamCounter.Next(0)

		streamNotify := rx.Observer(rx.Noop)
		rx.Observable(
			func(ctx context.Context, sink rx.Observer) {
				streamNotify = sink.Mutex()
			},
		).Pipe(
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
			if streamCounter.BlockingFirstOrDefault(mainCtx, 0).(int) > 0 {
				// Wait until `streamCounter` remains zero for N seconds.
				const N = 5

				println("waiting for remote streaming to complete...")

				_, _ = streamCounter.Pipe(
					operators.Filter(
						func(val interface{}, idx int) bool {
							return val == 0
						},
					),
					operators.SwitchMapTo(
						rx.Race(
							rx.Timer(N*time.Second).Pipe(operators.MapTo(nil)),
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
									// Check if this is the value from `rx.Timer`.
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

			_ = srv.Shutdown(mainCtx)

			if <-timeout {
				close(timeout)
			}
		}()

		go func() { _ = srv.Serve(l) }()
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
				if file.HasIncomplete() {
					return 1
				}

				return 0 // Successfully downloaded specified range.
			}
		}

		if app.streamToStdout {
			select {
			case <-streamRest:
			default:
				close(streamRest)
				select {
				case <-mainDone:
					return 1
				case <-streamDone:
				}
			}
		}

		if !app.Verify {
			return 0
		}

		type DigestInfo struct {
			Name string
			Hash hash.Hash
		}

		var digests map[string]DigestInfo

		for _, h := range supportedHashMethods {
			if h.IsAlias {
				continue
			}

			hashCode := h.Get(file)
			if hashCode != "" {
				if digests == nil {
					digests = make(map[string]DigestInfo)
				}

				digests[hashCode] = DigestInfo{h.Name, h.New()}
				println(h.Name+":", hashCode)
			}
		}

		shouldVerify := i == 0 || digests != nil
		if !shouldVerify {
			return 0
		}

		vs := rx.Observer(rx.Noop)
		rx.Observable(
			func(ctx context.Context, sink rx.Observer) {
				vs = sink
			},
		).Pipe(
			operators.SkipUntil(rx.Timer(time.Second)),
		).Subscribe(
			mainCtx,
			func(t rx.Notification) {
				switch {
				case t.HasValue:
					print("\033[1K\r")
					switch t.Value.(type) {
					case int64:
						printf("verifying...%v%%", t.Value)
					default:
						printf("verifying...%v\n", t.Value)
					}
				case t.HasError:
					print("\033[1K\r")
					printf("verifying...%v\n", t.Error)
				}
			},
		)

		p := int64(0)
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
			if s*100 >= (p+1)*contentSize {
				p = s * 100 / contentSize
				vs.Next(p)
			}

			return
		}

		err := file.Verify(mainCtx, WriterFunc(w))
		if err != nil {
			vs.Error(err)
			return 1
		}

		if file.CompleteSize() != contentSize {
			vs.Error(errors.New("BAD: file corrupted"))
			continue
		}

		for hashCode, digest := range digests {
			if hashCode != hex.EncodeToString(digest.Hash.Sum(nil)) {
				vs.Error(fmt.Errorf("BAD: %v mismatched", digest.Name))
				return 1
			}
		}

		if app.Autoremove {
			os.Remove(file.HashFile())
		}

		vs.Next("DONE")
		vs.Complete()

		return 0
	}

	return 1
}

func (app *App) dl(mainCtx context.Context, file *DataFile, client *http.Client) {
	type (
		Status int

		MeasureMessage struct{}

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

		StatusChangedMessage struct {
			Status Status
		}
	)

	const (
		StatusDownloading Status = iota
		StatusWaitStreaming
	)

	dlCtx := context.Background() // dlCtx never cancels.

	speedUpdateCtx, speedUpdateCancel := context.WithCancel(dlCtx)
	defer speedUpdateCancel()

	var emaValue, emaSpeed int64

	byteIntervals := rx.Multicast()
	rx.Observable(
		func(ctx context.Context, sink rx.Observer) {
			const N = 5
			skipZeros := byteIntervals.Pipe(
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
						// For the first `N * measureInterval`, we average the speed.
						emaValue = val.(int64) / int64(idx+1)
						emaSpeed = int64(float64(emaValue) / measureIntervalMultiple)

						return val
					},
				),
				operators.Take(N),
			).Subscribe(ctx, rx.Noop)
			skipZeros.Pipe(
				operators.BufferCountConfigure{
					BufferSize:       N,
					StartBufferEvery: 1,
				}.Make(),
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
						// After `N * measureInterval`, we calculate the speed by using
						// exponential moving average (EMA).
						const ia = 5 // Inverse of alpha.
						emaValue = (val.(int64) + (ia-1)*emaValue) / ia
						emaSpeed = int64(float64(emaValue) / measureIntervalMultiple)

						return val
					},
				),
				operators.TakeWhile(
					func(val interface{}, idx int) bool {
						return val.(int64) > 0
					},
				),
				operators.DoAfterErrorOrComplete(
					func() { emaValue, emaSpeed = 0, 0 },
				),
			).Subscribe(ctx, sink)
		},
	).Pipe(
		operators.RepeatForever(),
	).Subscribe(speedUpdateCtx, rx.Noop)

	var (
		firstRecvTime  time.Time
		nextReportTime time.Time
		totalReceived  int64
		recentReceived int64
		connections    int
		status         Status
		statusLine     string
	)

	handleMessagesCtx, handleMessagesCancel := context.WithCancel(dlCtx)
	queuedMessages := rx.Observer(rx.Noop)
	rx.Observable(
		func(ctx context.Context, sink rx.Observer) {
			// Since `Cache` is concurrency-safe, `sink.Mutex()` is not needed.
			queuedMessages = sink
		},
	).Pipe(
		operators.Cache(int(app.Connections*3)),
		operators.DoAfterErrorOrComplete(handleMessagesCancel),
	).Subscribe(handleMessagesCtx, func(t rx.Notification) {
		if t.HasValue {
			shouldPrint := false

			switch v := t.Value.(type) {
			case ProgressMessage:
				if totalReceived == 0 {
					firstRecvTime = time.Now()
					nextReportTime = firstRecvTime.Add(reportInterval)
				}

				totalReceived += int64(v.Just)
				recentReceived += int64(v.Just)
			case MeasureMessage:
				byteIntervals.Next(recentReceived)

				recentReceived = 0
				shouldPrint = file.ContentSize() > 0
			case ResponseMessage:
				connections++
			case CompleteMessage:
				if v.Responsed {
					connections--
				}
			case StatusChangedMessage:
				status = v.Status
			case string:
				print("\033[1K\r")
				println(v)

				statusLine = ""
				shouldPrint = file.ContentSize() > 0
			}

			if shouldPrint {
				var b strings.Builder

				contentSize := file.ContentSize()
				completeSize := file.CompleteSize()
				progress := int(float64(completeSize) / float64(contentSize) * 100)

				b.WriteString(fmt.Sprintf("%v%%", progress))
				b.WriteString(fmt.Sprintf(" %v", formatBytes(completeSize, "B")))
				b.WriteString(fmt.Sprintf("/%v", formatBytes(contentSize, "B")))

				switch {
				case connections > 0:
					b.WriteString(fmt.Sprintf(" CN:%v", connections))

					if emaSpeed > 0 {
						remaining := float64(file.IncompleteSize())
						seconds := int64(math.Ceil(remaining / float64(emaSpeed)))

						b.WriteString(fmt.Sprintf(" DL:%v", formatBytes(emaSpeed, "B/s")))
						b.WriteString(fmt.Sprintf(" ETA:%v", formatETA(time.Duration(seconds)*time.Second)))
					}
				case status == StatusDownloading:
					b.WriteString(" connecting...")
				case status == StatusWaitStreaming:
					b.WriteString(" streaming...")
				}

				if s := b.String(); s != statusLine {
					statusLine = s

					print("\033[1K\r")
					print(s)
				}
			}
		}

		if totalReceived > 0 && (!t.HasValue || time.Now().After(nextReportTime)) {
			nextReportTime = nextReportTime.Add(reportInterval)
			statusLine = ""

			timeUsed := time.Since(firstRecvTime)
			if timeUsed < time.Second {
				timeUsed = time.Second
			}

			print("\033[1K\r")
			printf(
				"recv %v in %v, %v, %v%% completed\n",
				formatBytes(totalReceived, "B"),
				formatTimeUsed(timeUsed),
				formatBytes(int64(float64(totalReceived)/timeUsed.Seconds()), "B/s"),
				int(float64(file.CompleteSize())/float64(file.ContentSize())*100),
			)
		}

		if !t.HasValue && statusLine != "" {
			println()
		}
	})

	defer func() {
		queuedMessages.Complete()
		<-handleMessagesCtx.Done()
	}()

	measureCtx, measureCancel := context.WithCancel(dlCtx)
	defer measureCancel()

	rx.Ticker(measureInterval).Pipe(
		operators.MapTo(MeasureMessage{}),
	).Subscribe(measureCtx, queuedMessages)

	handleWritesCtx, handleWritesCancel := context.WithCancel(dlCtx)
	queuedWrites := rx.Observer(rx.Noop)
	rx.Observable(
		func(ctx context.Context, sink rx.Observer) {
			// Since `Cache` is concurrency-safe, `sink.Mutex()` is not needed.
			queuedWrites = sink
		},
	).Pipe(
		operators.Cache(int(app.Connections*3)),
		operators.DoAfterErrorOrComplete(handleWritesCancel),
	).Subscribe(handleWritesCtx, func(t rx.Notification) {
		if t.HasValue {
			t.Value.(func())()
		}
	})

	defer func() {
		queuedWrites.Complete()
		<-handleWritesCtx.Done()

		_ = file.Sync()
	}()

	type buffer [readBufferSize]byte

	bufferPool := sync.Pool{
		New: func() interface{} { return new(buffer) },
	}

	readAndWrite := func(body io.Reader, offset int64) (n int, err error) {
		b := bufferPool.Get().(*buffer)

		n, err = body.Read((*b)[:])
		if n > 0 {
			queuedWrites.Next(func() {
				_, _ = file.WriteAt((*b)[:n], offset)
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

	messages := make(chan interface{}, app.Connections)

	handleTasksCtx, handleTasksCancel := context.WithCancel(dlCtx)
	activeTasks := rx.Observer(rx.Noop)
	rx.Observable(
		func(ctx context.Context, sink rx.Observer) {
			activeTasks = sink
		},
	).Pipe(
		operators.MergeAll(-1),
		operators.DoAfterErrorOrComplete(handleTasksCancel),
	).Subscribe(handleTasksCtx, queuedMessages)

	defer func() {
		activeTasks.Complete()

		done := handleTasksCtx.Done()

		for {
			select {
			case <-done:
				return
			case <-messages:
			}
		}
	}()

	var (
		activeCount     uint
		pauseNewTask    bool
		delayNewTask    <-chan time.Time
		streamCacheFull chan struct{}
		fatalErrors     bool
		errorCount      uint

		maxDownloads = app.Connections
		currentURL   = app.URL

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

	re1 := regexp.MustCompile(`^bytes (\d+)-(\d+)/(\d+)$`)
	re2 := regexp.MustCompile(`^bytes \*/(\d+)$`)
	errTryAgain := errors.New("try again")
	mainDone := mainCtx.Done()

	for activeCount > 0 || file.HasIncomplete() {
		switch {
		case activeCount >= maxDownloads:
		case pauseNewTask:
		case delayNewTask != nil:
		case streamCacheFull != nil:
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
				if currentURL == app.URL {
					if activeCount == 0 {
						// We tested all user agents, failed to start any download.
						return // Give up.
					}
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

			if app.streamToStdout && app.StreamCache > 0 {
				streamCacheSize := int64(app.StreamCache) * 1024 * 1024
				streamOffset := atomic.LoadInt64(app.streamOffset)

				if offset >= streamOffset+streamCacheSize {
					returnIncomplete(offset, size)

					streamCacheFull = make(chan struct{})

					queuedMessages.Next(StatusChangedMessage{StatusWaitStreaming})

					offset := offset

					go func() {
						defer close(streamCacheFull)

						ticker := time.NewTicker(time.Second)
						defer ticker.Stop()

						for {
							select {
							case <-mainDone:
								return
							case <-ticker.C:
								streamOffset := atomic.LoadInt64(app.streamOffset)
								if offset < streamOffset+streamCacheSize {
									return
								}
							}
						}
					}()

					break
				}
			}

			var userAgent string
			if len(userAgents) > 0 {
				userAgent = userAgents[0]
			}

			cr := func(parent context.Context, sink rx.Observer) {
				reqCtx, reqCancel := context.WithCancel(mainCtx) // Not parent.

				var (
					err   error
					fatal bool
				)

				defer func() {
					if err != nil {
						returnIncomplete(offset, size)
						sink.Next(CompleteMessage{err, fatal, false})
						sink.Complete()
						reqCancel()
					}
				}()

				req, err := http.NewRequest(http.MethodGet, currentURL, nil)
				if err != nil {
					return
				}

				req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+size-1))

				if app.Referer != "" {
					req.Header.Set("Referer", app.Referer)
				}

				req.Header.Set("User-Agent", userAgent)

				req = req.WithContext(reqCtx)

				resp, err := client.Do(req)
				if err != nil {
					return
				}

				defer func() {
					if err != nil {
						resp.Body.Close()
					}
				}()

				var contentSize int64

				switch resp.StatusCode {
				case http.StatusOK:
					if offset > 0 {
						err = errors.New("this server does not support partial requests")
						fatal = true

						return
					}

					contentSize = resp.ContentLength
				case http.StatusPartialContent:
					contentRange := resp.Header.Get("Content-Range")
					if contentRange == "" {
						err = errors.New("Content-Range not found")
						fatal = true

						return
					}

					slice := re1.FindStringSubmatch(contentRange)
					if slice == nil {
						err = fmt.Errorf("Content-Range unrecognized: %v", contentRange)
						fatal = true

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

					err = errors.New(resp.Status)
					fatal = true

					return
				default:
					err = errors.New(resp.Status)
					return
				}

				if contentSize <= 0 {
					err = errors.New("Content-Length unknown or zero")
					fatal = true

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
					err = errors.New("Content-Length mismatched")
					fatal = true

					return
				}

				for _, h := range supportedHashMethods {
					hashCode := resp.Header.Get("Content-" + h.Name)
					if hashCode == "" {
						hashCode = resp.Header.Get("X-Checksum-" + h.Name)
					}

					if len(hashCode) == h.Length {
						hashCode = strings.ToLower(hashCode)

						switch h.Get(file) {
						case hashCode:
						case "":
							h.Set(file, hashCode)

							shouldSync = true
						default:
							err = errors.New(h.Name + " mismatched")
							fatal = true

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
						fatal = true
						return
					}
				}

				if app.Truncate {
					_ = file.Truncate(contentSize)
				}

				if shouldSync {
					_ = file.SyncNow()
				}

				sink.Next(ResponseMessage{resp.Request.URL.String()})

				go func() {
					var result error

					var (
						readTimeout      = app.ReadTimeout
						readTimer        = time.AfterFunc(readTimeout, reqCancel)
						shouldResetTimer bool
						shouldWaitWrite  bool
					)

					defer func() {
						resp.Body.Close()

						if shouldWaitWrite {
							waitWriteDone()
						}

						returnIncomplete(offset, size)
						sink.Next(CompleteMessage{result, false, true})
						sink.Complete()
						reqCancel()
					}()

					for {
						if shouldResetTimer {
							readTimer.Reset(readTimeout)
						}

						n, err := readAndWrite(resp.Body, offset)

						readTimer.Stop()

						shouldResetTimer = true

						if n > 0 {
							offset, size = offset+int64(n), size-int64(n)
							shouldWaitWrite = true

							sink.Next(ProgressMessage{n})
						}

						if err != nil {
							if err == io.EOF {
								return
							}

							result = err

							return
						}
					}
				}()
			}

			do := func(t rx.Notification) {
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

			activeTasks.Next(rx.Observable(cr).Pipe(operators.Do(do)))
		}

		select {
		case <-mainDone:
			return
		case <-delayNewTask:
			delayNewTask = nil
		case <-streamCacheFull:
			streamCacheFull = nil

			queuedMessages.Next(StatusChangedMessage{StatusDownloading})
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

				if e.Responsed {
					if len(allUserAgents) > 1 {
						// Prepare to test all user agents again.
						userAgents = newUserAgents
						userAgents = append(userAgents, allUserAgents[userAgentIndex:]...)
						userAgents = append(userAgents, allUserAgents[:userAgentIndex]...)
					}
				} else { // There must be an error if no response.
					pauseNewTask = false

					switch {
					case e.Err == errTryAgain:
					case app.TimeoutIntolerant || !os.IsTimeout(e.Err):
						errorCount++

						if e.Fatal {
							fatalErrors = true
						}

						err := e.Err

						switch e := err.(type) {
						case *net.OpError:
							err = e.Err
						case *url.Error:
							err = e.Err
						}

						message := err.Error()
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
			}
		case <-syncTicker.C:
			_ = file.Sync()
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
		if h.IsAlias {
			continue
		}

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

func formatBytes(n int64, unit string) string {
	if n == 0 {
		return "0"
	}

	if n < 1024 {
		return strconv.FormatInt(n, 10) + unit
	}

	kilobytes := int64(math.Ceil(float64(n) / 1024))
	if kilobytes < 1000 {
		return strconv.FormatInt(kilobytes, 10) + "Ki" + unit
	}

	if kilobytes < 102400 {
		return strconv.FormatFloat(float64(n)/(1024*1024), 'f', 1, 64) + "Mi" + unit
	}

	megabytes := int64(math.Ceil(float64(n) / (1024 * 1024)))

	return strconv.FormatInt(megabytes, 10) + "Mi" + unit
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

func isDir(name string) bool {
	stat, err := os.Stat(name)
	if err != nil {
		return false
	}

	return stat.IsDir()
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

		jar.SetCookies(
			&url.URL{Scheme: scheme, Host: host},
			[]*http.Cookie{&cookie},
		)
	}

	return
}
