use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::read_dir;
use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::sync::mpsc::channel;
use std::thread;

const UNIGRAM_FILENAME: &str = "unigram_index.txt";
const BIGRAM_FILENAME: &str = "bigram_index.txt";

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
                    let s = String::from_utf8(word.unwrap())
                        .expect("unable to parse string")
                        .replace(",", " ")
                        .replace(".", " ")
                        .replace("?", " ")
                        .replace("!", " ")
                        .to_lowercase();

                    if s.is_empty() {
                        continue;
                    }

                    // though we split on a space, the 'word' may have
                    // contained punctuation that we've turned into a space.
                    // So now we need to iterate through the inner subwords & send each.
                    for subword in s.split_ascii_whitespace() {
                        // send work
                        child_send_ch
                            .send((subword.to_string(), filename.clone()))
                            .expect("Error sending stuff back to main");
                    }
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

    let mut out_file = BufWriter::new(File::create(UNIGRAM_FILENAME).unwrap());

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
                    let s = String::from_utf8(word.unwrap())
                        .expect("unable to parse string")
                        .replace("\t", "")
                        .replace(",", " ")
                        .replace(".", " ")
                        .replace("?", " ")
                        .replace("!", " ")
                        .to_lowercase();

                    if s.is_empty() {
                        continue;
                    }

                    match prv_wrd {
                        None => {
                            let mut it = s.split_ascii_whitespace();
                            let first = it.next();
                            if first.is_none() {
                                continue;
                            }

                            let mut first = first.unwrap();

                            for word in it {
                                child_send_ch
                                    .send((first.to_owned() + " " + word, filename.clone()))
                                    .expect("Error sending first words back to main");

                                first = word;
                            }

                            prv_wrd = Some(first.to_owned());
                        }
                        Some(p) => {
                            let mut curr = p;

                            for subword in s.split_ascii_whitespace() {
                                child_send_ch
                                    .send((curr + " " + subword, filename.clone()))
                                    .expect("Error sending first words back to main");

                                curr = subword.to_owned();
                            }

                            prv_wrd = Some(curr);
                        }
                    }

                    for subword in s.split_ascii_whitespace() {
                        // send work

                        child_send_ch
                            .send((subword.to_string(), filename.clone()))
                            .expect("Error sending stuff back to main");
                    }
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

    let mut out_file = BufWriter::new(File::create(BIGRAM_FILENAME).unwrap());

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
    let unigram_file = "fulldata";
    let bigram_file = "devdata";

    unigram_gen(unigram_file);
    bigram_gen(bigram_file);
}
