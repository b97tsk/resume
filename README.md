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

# Files

### Input Files

- `Configure` (optional) an YAML document contains any of following options:
  - `url` the URL you want to download.
  - `referer` the referer.
  - `split-size` split size.
  - `concurrent` maximum number of parallel downloads.
  - `error-capacity` maximum number of errors.
  - `request-interval` request interval.
  - `request-range` request range.
  - `user-agents` list of user agents.
  - `per-user-agent-limit` limit per user agent connections.
  - `stream-rate` maximum number of stream rate.
- `Cookies` (optional) the cookies. The file format is [Netscape format](https://unix.stackexchange.com/a/210282).

> Note that command line arguments take precedence over `Configure`.

### Output Files

- `File` all the data downloaded from the URL.
- `Hash` hash information of the `File`.

> If `File` or `Hash` is missing, download will start from the beginning.
