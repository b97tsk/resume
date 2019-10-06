# resume

Splitting download a file.

# Install

```
go get -u github.com/b97tsk/resume
```

# Usage

```console
### Create a working directory.
$ mkdir work && cd work
### Download with max concurrent number set to 9.
$ resume -c 9 http://...
### A file named `File` will be created, save it when done.
$ cp File path/to/file
### Remove the working directory if you don't need it anymore.
$ cd .. && rm -rf work
```

# Configure File

By default, a file named `resume.yaml` is read when the program starts, if it exists.
You can specify the path of this configure file by using command line flag `-f`.

A configure file is an YAML document which can specify following options:

- `url` the URL you want to download.
- `output` output file.
- `split` split size.
- `connections` maximum number of parallel downloads.
- `errors` maximum number of errors.
- `sync-period` sync-to-disk period.
- `interval` request interval.
- `range` request range.
- `cookie` cookie file. The file format is [Netscape format](https://unix.stackexchange.com/a/210282).
- `referer` the referer.
- `user-agents` list of user agents.
- `per-user-agent-limit` limit per user agent connections.
- `stream-rate` maximum number of stream rate.
- `alloc` alloc disk space before the first write.
- `skip-etag` skip unreliable ETag field or not.
- `skip-last-modified` skip unreliable Last-Modified field or not.
- `remote-control` create an HTTP server for remote control.

> Note that command line arguments take precedence over this configure file.
