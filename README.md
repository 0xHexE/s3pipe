# S3Pipe

Example downloader_config.yaml

```yaml
downloadPath: "../downloaded_files"
databasePath: "./torrent_files.db"
enableUPnP: false
listenPort: 6881
enableProgress: true
maxParallel: 5
checkInterval: 5
s3pipeBaseURL: "http://localhost:8181"
s3pipeToken: ""
exposeStream:
  type: "http-server"
  config:
    url: "http://localhost:8080"
    post:
      - type: "s3"
        endpoint: ""
        access_token: ""
        secret_key: ""
        region: ""
```
