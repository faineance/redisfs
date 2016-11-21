#[macro_use]
extern crate log;
extern crate env_logger;
extern crate fuse;
extern crate redis;
extern crate libc;
extern crate time;
use time::Timespec;
use std::path::Path;
use redis::Commands;
use std::collections::HashMap;
use std::env;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyWrite,
           ReplyOpen, ReplyDirectory};

use libc::{ENOENT, ENOSYS};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

const DEFAULT_TIME: Timespec = Timespec { sec: 0, nsec: 0 };

pub const BLOCK_SIZE: u32 = 4096;

const ROOT_ATTR: fuse::FileAttr = fuse::FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: DEFAULT_TIME,
    mtime: DEFAULT_TIME,
    ctime: DEFAULT_TIME,
    crtime: DEFAULT_TIME,
    kind: fuse::FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 0,
    gid: 0,
    rdev: 0,
    flags: 0,
};

struct RedisFS {
    redis_connection: redis::Connection,
}

impl RedisFS {
    fn new(redis_connection_string: &str) -> redis::RedisResult<RedisFS> {
        let client = try!(redis::Client::open(redis_connection_string));
        Ok(RedisFS { redis_connection: try!(client.get_connection()) })
    }

    fn get_key_vals_ino(&self) -> HashMap<u64, (String, String)> {

        let keys: Vec<String> = self.redis_connection.keys("*").unwrap();
        let filtered_keys: Vec<String> = keys.into_iter()
            .filter(|key| {
                let key_type: String =
                    redis::cmd("TYPE").arg(key).query(&self.redis_connection).unwrap();
                key_type == "string"
            })
            .collect::<Vec<_>>();
        let mut key_val_pairs = HashMap::new();

        for (index, key) in filtered_keys.into_iter().enumerate() {
            let val: String = self.redis_connection.get(&key).unwrap();
            key_val_pairs.insert((index + 2) as u64, (key, val));
        }
        key_val_pairs

    }
}

impl Filesystem for RedisFS {
    fn statfs(&mut self, _req: &fuse::Request, _ino: u64, reply: fuse::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, BLOCK_SIZE, 256, 0);
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        if parent == 1 {
            for (ino, (key, val)) in self.get_key_vals_ino() {

                if key == name.to_str().unwrap() {
                    let attr = FileAttr {
                        ino: ino,
                        size: 13,
                        blocks: 1,
                        atime: DEFAULT_TIME,
                        mtime: DEFAULT_TIME,
                        ctime: DEFAULT_TIME,
                        crtime: DEFAULT_TIME,
                        kind: FileType::RegularFile,
                        perm: 0o644,
                        nlink: 1,
                        uid: 501,
                        gid: 20,
                        rdev: 0,
                        flags: 0,
                    };
                    reply.entry(&TTL, &attr, 0);
                    return;
                }

            }
        }
        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let s = self.get_key_vals_ino();
        if ino == 1 {
            reply.attr(&TTL, &ROOT_ATTR);
            return;
        }
        match s.get(&ino) {
            Some(keyval) => {
                let attr = FileAttr {
                    ino: ino,
                    size: 13,
                    blocks: 1,
                    atime: DEFAULT_TIME,
                    mtime: DEFAULT_TIME,
                    ctime: DEFAULT_TIME,
                    crtime: DEFAULT_TIME,
                    kind: FileType::RegularFile,
                    perm: 0o644,
                    nlink: 1,
                    uid: 501,
                    gid: 20,
                    rdev: 0,
                    flags: 0,
                };
                reply.attr(&TTL, &attr);
            }
            None => reply.error(ENOENT),
        }
    }

    fn read(&mut self,
            _req: &Request,
            ino: u64,
            _fh: u64,
            offset: u64,
            _size: u32,
            reply: ReplyData) {
        let s = self.get_key_vals_ino();
        match s.get(&ino) {
            Some(keyval) => {
                reply.data(&keyval.1.as_bytes()[offset as usize..]);
            }
            None => reply.error(ENOENT),
        }

    }

    fn open(&mut self, _req: &fuse::Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn write(&mut self,
             _req: &Request,
             ino: u64,
             _fh: u64,
             _offset: u64,
             _data: &[u8],
             _flags: u32,
             reply: ReplyWrite) {
        println!("{:?} {:?} {:?} {:?} {:?} {:?}",
                 _req,
                 ino,
                 _fh,
                 _offset,
                 _data,
                 _flags);
        let s = self.get_key_vals_ino();
        match s.get(&ino) {
            Some(keyval) => {

                reply.written(10)
                // reply.data(&keyval.1.as_bytes()[offset as usize..]);
            }
            None => reply.error(ENOENT),
        }

    }

    fn readdir(&mut self,
               _req: &Request,
               ino: u64,
               _fh: u64,
               offset: u64,
               mut reply: ReplyDirectory) {

        if ino == 1 {
            if offset == 0 {
                reply.add(1, 0, FileType::Directory, ".");
                reply.add(1, 1, FileType::Directory, "..");

                for (ino, (key, val)) in self.get_key_vals_ino() {
                    reply.add(ino, 2, FileType::RegularFile, key);
                }
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }

    }
}



fn main() {
    env_logger::init().unwrap();

    let mountpoint = env::args_os().nth(1).expect("No mount path given");
    let redis_connection_string = env::args_os().nth(2).expect("No redis host given");
    fuse::mount(RedisFS::new(redis_connection_string.to_str().unwrap()).unwrap(),
                &mountpoint,
                &[]);
}