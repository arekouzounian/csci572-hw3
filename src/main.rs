use std::cmp::min;
use std::collections::HashMap;
use std::fs::read_dir;
use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::sync::mpsc::channel;
use std::thread;

use regex::Regex;

// TODO:
//  - need to replace all punctuation and numerals with spaces
//  - can do a first pass and save data intermediately
//  - alternative: each mapper still splits on whitespace but also splits internal characters

fn unigram_gen(path: &str) {
    let dir = read_dir(path).expect("Invalid directory!");
    let mut paths = vec![];
    let mut children = vec![];
    let mut child_channels = vec![];
    const MAX_MAPPERS: usize = 8;

    let (child_send, global_recv) = channel();

    for item in dir {
        match item {
            Ok(item) => paths.push(item.path()),
            Err(_) => continue,
        }
    }

    let num_mappers = min(paths.len(), MAX_MAPPERS);

    for _ in 0..num_mappers {
        let (main_send, child_recv) = channel::<std::path::PathBuf>();
        child_channels.push(main_send);
        let child_send_ch = child_send.clone();
        children.push(thread::spawn(move || {
            let re = Regex::new(r"^[[:punct:]]+|[[:punct:]]+$").unwrap();

            for path in child_recv {
                // do work
                let f = File::open(path).unwrap();
                let mut reader = std::io::BufReader::new(f);
                let mut filename = vec![];

                reader
                    .read_until(b'\t', &mut filename)
                    .expect("no tab delimiter!");

                let filename = String::from_utf8(filename).unwrap().replace("\t", "");

                for word in reader.split(b' ') {
                    // remove tabs
                    let mut s = String::from_utf8(word.unwrap())
                        .expect("unable to parse string")
                        .replace("\t", "");

                    // remove punctuation
                    s = re.replace(&s.trim(), "").to_string().to_lowercase();

                    if s.len() < 1 {
                        continue;
                    }

                    // send work
                    child_send_ch
                        .send((s, filename.clone()))
                        .expect("Error sending stuff back to main");
                }
            }
        }))
    }

    drop(child_send); // drop original send channel

    // send paths to each mapper
    let mut curr_mapper = 0;
    for path in paths {
        child_channels[curr_mapper]
            .send(path)
            .expect("Error sending path to child");

        curr_mapper += 1;
        curr_mapper %= child_channels.len();
    }

    // close channels to indicate we're done sending
    drop(child_channels);

    println!("finished sending to all mappers. reducing...");

    let mut map: HashMap<String, HashMap<String, i32>> = HashMap::new();

    // reduce
    for (word, doc) in global_recv {
        // println!("{word} {doc}");

        if !map.contains_key(&word) {
            map.insert(word.clone(), HashMap::new());
        }

        let idx = map.get_mut(&word).unwrap();
        if idx.contains_key(&doc) {
            let val = idx.get_mut(&doc).unwrap();
            *val += 1;
        } else {
            idx.insert(doc, 1);
        }
    }

    println!("reduction complete. writing...");

    let mut out_file = BufWriter::new(File::create("unigrams.txt").unwrap());

    for (wrd, dict) in map {
        // println!("{}", wrd);
        let mut line = String::from("");
        for (doc, count) in dict {
            line.push_str(&format!("{}:{} ", doc, count));
        }
        out_file
            .write_fmt(format_args!("{}\t{}\n", wrd, line))
            .unwrap();
    }

    out_file.flush().unwrap();
    println!("writing complete.");

    // join
    for child in children {
        child.join().expect("child thread panicked before joining");
    }
}

fn bigram_gen(path: &str) {
    let dir = read_dir(path).expect("Invalid directory!");
    let mut paths = vec![];
    let mut children = vec![];
    let mut child_channels = vec![];
    const MAX_MAPPERS: usize = 8;

    let (child_send, global_recv) = channel();

    for item in dir {
        match item {
            Ok(item) => paths.push(item.path()),
            Err(_) => continue,
        }
    }

    let num_mappers = min(paths.len(), MAX_MAPPERS);

    for _ in 0..num_mappers {
        let (main_send, child_recv) = channel::<std::path::PathBuf>();
        child_channels.push(main_send);
        let child_send_ch = child_send.clone();
        children.push(thread::spawn(move || {
            let re = Regex::new(r"^[[:punct:]]+|[[:punct:]]+$").unwrap();

            for path in child_recv {
                // do work
                let f = File::open(path).unwrap();
                let mut reader = std::io::BufReader::new(f);
                let mut filename = vec![];

                reader
                    .read_until(b'\t', &mut filename)
                    .expect("no tab delimiter!");

                let filename = String::from_utf8(filename).unwrap().replace("\t", "");

                let mut prv_wrd: Option<String> = None;

                for word in reader.split(b' ') {
                    // remove tabs
                    let mut s = String::from_utf8(word.unwrap())
                        .expect("unable to parse string")
                        .replace("\t", "");

                    // remove punctuation
                    s = re.replace(&s.trim(), "").to_string().to_lowercase();

                    if s.len() < 1 {
                        continue;
                    }

                    if let Some(p) = prv_wrd {
                        let tmp = s.clone();

                        s = p + " " + &s;

                        prv_wrd = Some(tmp);
                    } else {
                        prv_wrd = Some(s);
                        continue;
                    }

                    // send work
                    child_send_ch
                        .send((s, filename.clone()))
                        .expect("Error sending stuff back to main");
                }
            }
        }))
    }

    drop(child_send); // drop original send channel

    // send paths to each mapper
    let mut curr_mapper = 0;
    for path in paths {
        child_channels[curr_mapper]
            .send(path)
            .expect("Error sending path to child");

        curr_mapper += 1;
        curr_mapper %= child_channels.len();
    }

    // close channels to indicate we're done sending
    drop(child_channels);

    println!("finished sending to all mappers. reducing...");

    let mut map: HashMap<String, HashMap<String, i32>> = HashMap::new();

    // reduce
    for (word, doc) in global_recv {
        // println!("{word} {doc}");

        if !map.contains_key(&word) {
            map.insert(word.clone(), HashMap::new());
        }

        let idx = map.get_mut(&word).unwrap();
        if idx.contains_key(&doc) {
            let val = idx.get_mut(&doc).unwrap();
            *val += 1;
        } else {
            idx.insert(doc, 1);
        }
    }

    println!("reduction complete. writing...");

    let mut out_file = BufWriter::new(File::create("bigrams.txt").unwrap());

    for (wrd, dict) in map {
        // println!("{}", wrd);
        let mut line = String::from("");
        for (doc, count) in dict {
            line.push_str(&format!("{}:{} ", doc, count));
        }
        out_file
            .write_fmt(format_args!("{}\t{}\n", wrd, line))
            .unwrap();
    }

    out_file.flush().unwrap();
    println!("writing complete.");

    // join
    for child in children {
        child.join().expect("child thread panicked before joining");
    }
}

fn main() {
    let file = "fulldata";

    unigram_gen(file);
    bigram_gen(file);
}
