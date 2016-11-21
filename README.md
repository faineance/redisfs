# redisfs
Mount redis as a filesystem.

```shell
$ cargo run -- ~/mnt/ redis://127.0.0.1/ &
$ redis-cli
127.0.0.1:6379> set hello_key world_value
OK
$ ls ~/mnt 
hello_key
$ cat ~/mnt/hello_key
world_value
$ echo foobar >> ~/mnt/hello_key
$ redis-cli
127.0.0.1:6379> get hello_key
"world_valuefoobar\n"
```

todo:
- make more writeable
