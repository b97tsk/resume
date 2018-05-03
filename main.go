package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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
		Err error
	}
)

var (
	_url             string
	_referer         string
	_userAgent       string
	_contentLength   int64
	_contentMD5      string
	_entityTag       string
	_lastModified    string
	_splitSize       uint
	_concurrent      uint
	_errorCapacity   uint
	_requestInterval time.Duration
)

func main() {
	flag.UintVar(&_splitSize, "s", _defaultSplitSize, "split size (MiB)")
	flag.UintVar(&_concurrent, "c", _defaultConcurrent, "maximum number of parallel downloads")
	flag.UintVar(&_errorCapacity, "e", _defaultErrorCapacity, "maximum number of errors")
	flag.DurationVar(&_requestInterval, "i", _defaultRequestInterval, "request interval")
	flag.Parse()

	rawurl, err := _loadURL(filepath.Join(".", "URL"))
	if err != nil {
		fmt.Println(err)
		return
	}
	_url = rawurl

	_referer, _ = _loadSingleLine(filepath.Join(".", "Referer"), 1024)
	_userAgent, _ = _loadSingleLine(filepath.Join(".", "UserAgent"), 1024)

	contentLength, err := _loadSingleLine(filepath.Join(".", "ContentLength"), 16)
	if err == nil {
		i, err := strconv.ParseInt(contentLength, 10, 0)
		if err == nil {
			_contentLength = i
		}
	}

	_contentMD5, _ = _loadSingleLine(filepath.Join(".", "ContentMD5"), 36)
	_entityTag, _ = _loadSingleLine(filepath.Join(".", "ETag"), 64)
	_lastModified, _ = _loadSingleLine(filepath.Join(".", "LastModified"), 36)

	jar, _ := _loadCookies(filepath.Join(".", "Cookies"))
	client := &http.Client{Jar: jar}

	fileName := filepath.Join(".", "File")
	fileSize := int64(0)
	if info, err := os.Stat(fileName); err == nil {
		fileSize = info.Size()
	}

	file, err := _openDataFile(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	if fileSize > 0 {
		fmt.Print("verifying...")
		file.LoadHashFile()
	}
	if _contentLength > 0 {
		file.SetFileSize(_contentLength)
	}

	topCtx := context.TODO()

	activeCtx, activeCancel := context.WithCancel(topCtx)
	activeDone := activeCtx.Done()
	defer activeCancel()

	activeCount := uint32(0)

	cr := func(parent context.Context, ob observable.Observer) (ctx context.Context, cancel context.CancelFunc) {
		ctx, cancel = context.WithCancel(parent)
		defer func() {
			activeCancel()
			ob.Complete()
			cancel()
		}()

		var (
			stateChanged = make(chan struct{}, 1)
			beingDelayed <-chan time.Time
			beingPaused  uint32
			shouldDelay  uint32
			errorCount   uint32
		)

		ob.Next(observable.Interval(time.Second).TakeUntil(observable.Create(
			func(ctx context.Context, ob observable.Observer) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(ctx)
				go func() {
					defer cancel()
					<-activeDone
					ob.Complete()
				}()
				return ctx, cancel
			},
		)).MapTo(_PrintMessage{}))

		takeIncomplete := func() (offset, size int64) {
			splitSize := int64(_splitSize) * 1024 * 1024
			if _contentLength > 0 {
				size := (_contentLength - file.CompleteSize()) / int64(_concurrent)
				if size < splitSize {
					splitSize = size
				}
			}
			return file.TakeIncomplete(splitSize)
		}

		returnIncomplete := func(offset, size int64) {
			file.ReturnIncomplete(offset, size)
		}

		for {
			switch {
			case atomic.LoadUint32(&beingPaused) != 0:
			case beingDelayed != nil:

			case atomic.LoadUint32(&shouldDelay) != 0:
				atomic.StoreUint32(&shouldDelay, 0)
				beingDelayed = time.After(_requestInterval)

			case atomic.LoadUint32(&errorCount) >= uint32(_errorCapacity):
				if atomic.LoadUint32(&activeCount) == 0 {
					return
				}
			case atomic.LoadUint32(&activeCount) >= uint32(_concurrent):

			default:
				offset, size := takeIncomplete()
				if size == 0 {
					if atomic.LoadUint32(&activeCount) == 0 {
						return
					}
					break
				}

				cr := func(parent context.Context, ob observable.Observer) (ctx context.Context, cancel context.CancelFunc) {
					ctx, cancel = context.WithCancel(activeCtx)

					var err error
					defer func() {
						if err != nil {
							returnIncomplete(offset, size)
							ob.Next(_CompleteMessage{err})
							ob.Complete()
							cancel()
						}
					}()

					req, err := http.NewRequest(http.MethodGet, _url, nil)
					if err != nil {
						return
					}

					shouldLimitSize := false
					if _contentLength > 0 {
						req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+size-1))
					} else {
						req.Header.Set("Range", fmt.Sprintf("bytes=%v-", offset))
						shouldLimitSize = true
					}

					if _entityTag != "" {
						req.Header.Set("If-Range", _entityTag)
					} else if _lastModified != "" {
						req.Header.Set("If-Range", _lastModified)
					}

					if _referer != "" {
						req.Header.Set("Referer", _referer)
					}
					if _userAgent != "" {
						req.Header.Set("User-Agent", _userAgent)
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

					if resp.StatusCode != 206 {
						err = fmt.Errorf("range request failed: %v", resp.Status)
						return
					}

					contentRange := resp.Header.Get("Content-Range")
					if contentRange == "" {
						err = errors.New("Content-Range not found")
						return
					}

					re := regexp.MustCompile(`^bytes (\d+)-(\d+)/(\d+)$`)
					slice := re.FindStringSubmatch(contentRange)
					if slice == nil {
						err = fmt.Errorf("Content-Range unrecognized: %v", contentRange)
						return
					}

					contentLength, _ := strconv.ParseInt(slice[3], 10, 0)
					err = _setContentLength(contentLength)
					if err != nil {
						return
					}
					err = _setContentMD5(resp.Header.Get("Content-MD5"))
					if err != nil {
						return
					}

					if eTag := resp.Header.Get("ETag"); eTag != "" {
						err = _setEntityTag(eTag)
						if err != nil {
							return
						}
					} else if lastModified := resp.Header.Get("Last-Modified"); lastModified != "" {
						err = _setLastModified(lastModified)
						if err != nil {
							return
						}
					}

					if req != resp.Request {
						_url = resp.Request.URL.String()
					}

					file.SetFileSize(contentLength)

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
						defer func() {
							resp.Body.Close()
							returnIncomplete(offset, size)
							ob.Next(_CompleteMessage{err})
							ob.Complete()
							cancel()
						}()

						var (
							buf              = make([]byte, _readBufferSize)
							readTimer        = time.AfterFunc(_readTimeout, cancel)
							shouldResetTimer bool
						)

						for {
							if shouldResetTimer {
								readTimer.Reset(_readTimeout)
							}
							n, err := body.Read(buf)
							readTimer.Stop()
							shouldResetTimer = true
							if n > 0 {
								if _, err := file.WriteAt(buf[:n], offset); err != nil {
									return err
								}
								offset, size = offset+int64(n), size-int64(n)
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

							select {
							case stateChanged <- struct{}{}:
							default:
							}
						}
					}
				}

				atomic.StoreUint32(&beingPaused, 1)
				atomic.StoreUint32(&shouldDelay, 1)
				atomic.AddUint32(&activeCount, 1)

				ob.Next(observable.Create(cr).Do(observable.ObserverFunc(do)))
			}

			select {
			case <-activeDone:
				return
			case <-stateChanged:
			case <-beingDelayed:
				beingDelayed = nil
			}
		}
	}

	type stat struct {
		Time time.Time
		Size int64
	}

	const statCapacity = 5

	var (
		statList    = make([]stat, 0, statCapacity)
		statCurrent = stat{time.Now(), 0}
	)

	do := func(t observable.Notification) {
		switch {
		case t.HasValue:
			shouldPrint := false

			switch v := t.Value.(type) {
			case _ProgressMessage:
				statCurrent.Size += int64(v.Just)

			case _PrintMessage:
				if len(statList) == statCapacity {
					copy(statList, statList[1:])
					statList = statList[:statCapacity-1]
				}
				statList = append(statList, statCurrent)
				statCurrent = stat{time.Now(), 0}
				shouldPrint = true

			case _CompleteMessage:
				switch v.Err {
				case nil, io.ErrUnexpectedEOF, context.Canceled:
				default:
					fmt.Print("\r\033[K")
					fmt.Println(v.Err)
					shouldPrint = true
				}
			}

			if shouldPrint {
				fmt.Print("\r\033[K")

				if _contentLength > 0 {
					const length = 20
					n := int(float64(file.CompleteSize()) / float64(_contentLength) * 100)
					fmt.Printf("%v%% [%-*s]", n, length, strings.Repeat("=", length*n/100))
				}

				activeCount := atomic.LoadUint32(&activeCount)
				if activeCount > 0 {
					stat := statCurrent
					for _, s := range statList {
						stat.Size += s.Size
					}
					if len(statList) > 0 {
						stat.Time = statList[0].Time
					}
					bytes := float64(stat.Size)
					seconds := time.Since(stat.Time).Seconds()
					fmt.Printf(" DL:%v %vB/s", activeCount, _formatBytes(int64(bytes/seconds)))
				}
			}

		case t.HasError:

		default:
			fmt.Print("\r\033[K")
			if _contentLength > 0 {
				const length = 20
				n := int(float64(file.CompleteSize()) / float64(_contentLength) * 100)
				fmt.Printf("%v%% [%-*s]\n", n, length, strings.Repeat("=", length*n/100))
			}
		}
	}

	ctx, _ := observable.Create(cr).MergeAll().Subscribe(topCtx, observable.ObserverFunc(do))
	done := ctx.Done()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-c:
		activeCancel()
	case <-done:
	}

	signal.Stop(c)
	<-done
}

func _formatBytes(n int64) string {
	if n < 1 {
		return "0"
	}
	if n < 1024 {
		return "<1K"
	}
	kilobytes := int64(math.Ceil(float64(n) / 1024))
	if kilobytes < 1000 {
		return fmt.Sprintf("%vK", kilobytes)
	}
	if kilobytes < 102400 {
		return fmt.Sprintf("%.1fM", float64(n)/(1024*1024))
	}
	megabytes := int64(math.Ceil(float64(n) / (1024 * 1024)))
	return fmt.Sprintf("%vM", megabytes)
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
	rawurl, err = _loadSingleLine(name, 1024)
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

		expires, err := strconv.ParseInt(fields[4], 10, 0)
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

		u, err := url.Parse(fmt.Sprintf("%v://%v/", scheme, host))
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

func _storeSingleLine(name string, line string) error {
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, _filePerm)
	if err != nil {
		return err
	}
	_, werr := file.WriteString(line)
	serr := file.Sync()
	cerr := file.Close()
	if werr != nil {
		return werr
	}
	if serr != nil {
		return serr
	}
	return cerr
}

func _setContentLength(contentLength int64) error {
	if _contentLength != contentLength {
		if _contentLength != 0 {
			return errors.New("Content-Length mismatched")
		}
		_contentLength = contentLength
		_storeSingleLine(
			filepath.Join(".", "ContentLength"),
			strconv.FormatInt(_contentLength, 10),
		)
	}
	return nil
}

func _setContentMD5(contentMD5 string) error {
	if _contentMD5 != contentMD5 {
		if _contentMD5 != "" {
			return errors.New("Content-MD5 mismatched")
		}
		_contentMD5 = contentMD5
		_storeSingleLine(filepath.Join(".", "ContentMD5"), _contentMD5)
	}
	return nil
}

func _setEntityTag(eTag string) error {
	if _entityTag != eTag {
		if _entityTag != "" {
			return errors.New("ETag mismatched")
		}
		_entityTag = eTag
		_storeSingleLine(filepath.Join(".", "ETag"), _entityTag)
	}
	return nil
}

func _setLastModified(lastModified string) error {
	if _lastModified != lastModified {
		if _lastModified != "" {
			return errors.New("Last-Modified mismatched")
		}
		_lastModified = lastModified
		_storeSingleLine(filepath.Join(".", "LastModified"), _lastModified)
	}
	return nil
}
