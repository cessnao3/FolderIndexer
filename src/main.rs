use clap::Parser;
use md5::{Digest, Md5};
use relative_path::{RelativePathBuf, RelativePath};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    io::Read,
    path::{Path, PathBuf}, sync::Mutex, sync::Arc,
};

#[derive(Clone, Copy, Debug, clap::ValueEnum, Eq, PartialEq)]
enum ExistingFileAction {
    Nothing,
    Check,
    Update,
}

/// File Indexing Program
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Database file
    #[clap(value_parser)]
    db_file: PathBuf,

    /// Folders to include
    #[clap(short, long, value_enum)]
    folders: Vec<String>,

    /// Allow removing old entries
    #[clap(short, long, value_parser, default_value_t = false)]
    remove_old_entries: bool,

    /// Check all entries
    #[clap(short, long, value_enum, default_value_t = ExistingFileAction::Nothing)]
    existing: ExistingFileAction,

    /// Includes files and folders that start with a dot in the database
    #[clap(short, long, value_parser, default_value_t = false)]
    include_dot_files: bool,

    /// Determines how many threads run at the same time
    #[clap(short, long, value_parser, default_value_t = 0)]
    processes: usize,
}

fn find_files_in_directory(p: &Path, args: &Args) -> Vec<PathBuf> {
    if !p.is_dir() {
        panic!("path {} is not a directory!", p.to_str().unwrap());
    }

    let mut files = Vec::new();

    for entry in std::fs::read_dir(p).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            files.extend(find_files_in_directory(&path, args));
        } else if path.file_name().unwrap().to_str().unwrap().starts_with('.')
            && !args.include_dot_files
        {
            // Skip dot files
            continue;
        } else {
            files.push(path);
        }
    }

    files
}

fn compute_hash(p: &Path) -> String {
    let mut f = std::fs::File::open(p).unwrap();

    let mut buf = [0u8; 81920];

    let mut hasher = Md5::new();

    loop {
        let read_len = f.read(&mut buf).unwrap();
        if read_len == 0 {
            break;
        }

        hasher.update(&buf[..read_len]);
    }

    hasher
        .finalize()
        .iter()
        .map(|v| format!("{v:02x}"))
        .reduce(|a, b| format!("{a}{b}"))
        .unwrap()
}

struct FileDatabase {
    files: HashMap<RelativePathBuf, String>,
    change_count: usize,
}

impl FileDatabase {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            change_count: 0,
        }
    }

    pub fn load(path: &Path) -> Self {
        Self {
            files: std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .filter_map(|s| s.trim().split_once(' '))
                .map(|(h, d)| (RelativePathBuf::from(d), h.to_string()))
                .collect::<HashMap<_, _>>(),
            change_count: 0,
        }
    }

    pub fn add_file(&mut self, f: &RelativePath, hash: &str) {
        self.files.insert(f.to_owned(), hash.to_owned());
        self.change_count += 1;
    }

    pub fn get_hash(&self, f: &RelativePath) -> Option<&str> {
        self.files.get(f).map(|s| s.as_ref())
    }

    pub fn save(&mut self, path: &Path) {
        let mut file_list = self.files.iter().collect::<Vec<_>>();
        file_list.sort_by(|a, b| a.0.cmp(b.0));

        std::fs::write(
            path,
            file_list
                .iter()
                .map(|(p, h)| format!("{h} {p}"))
                .reduce(|a, b| format!("{a}\n{b}"))
                .unwrap_or(String::new()),
        )
        .unwrap();

        self.change_count = 0;
    }

    pub fn truncate_to_existing(&mut self, files: &BTreeSet<RelativePathBuf>) {
        let remove_files = self.files
            .keys()
            .filter(|k| !files.contains(*k))
            .cloned()
            .collect::<Vec<_>>();

        for f in remove_files {
            self.files.remove(&f);
            println!("Removing {f}");
            self.change_count += 1;
        }
    }

    pub fn num_changes(&self) -> usize {
        self.change_count
    }

    pub fn has_changes(&self) -> bool {
        self.change_count != 0
    }
}

#[derive(Clone)]
struct ThreadArgs {
    base_path: PathBuf,
    existing_action: ExistingFileAction,
    db_file: PathBuf,
    fail_due_to_difference: Arc<Mutex<bool>>,
    file_db: Arc<Mutex<FileDatabase>>,
    input_queue: Arc<Mutex<VecDeque<RelativePathBuf>>>,
}

impl ThreadArgs {
    pub fn new(args: &Args, base_path: &Path, files: VecDeque<RelativePathBuf>) -> Self {
        let file_db = if args.db_file.exists() {
            FileDatabase::load(&args.db_file)
        } else {
            FileDatabase::new()
        };

        Self {
            base_path: base_path.to_owned(),
            existing_action: args.existing,
            db_file: args.db_file.to_owned(),
            fail_due_to_difference: Arc::new(Mutex::new(false)),
            file_db: Arc::new(Mutex::new(file_db)),
            input_queue: Arc::new(Mutex::new(files)),
        }
    }
}

fn process_file(args: &ThreadArgs, f: &RelativePath) {
    let f_path = f.to_path(&args.base_path);

    let existing_hash = args.file_db.lock().unwrap().get_hash(f).map(|s| s.to_owned());
    let mut db_hash = None;

    if let Some(old_hash) = existing_hash {
        if args.existing_action != ExistingFileAction::Nothing {
            println!("Computing {f}");
            let new_hash = compute_hash(&f_path);

            if new_hash != old_hash {
                println!("  Mismatch in hash for {f} => old {old_hash}, new {new_hash}");

                match args.existing_action {
                    ExistingFileAction::Update => {
                        db_hash = Some(("  Updating!", new_hash));
                    }
                    ExistingFileAction::Check => {
                        *args.fail_due_to_difference.lock().unwrap() = true;
                    }
                    _ => panic!(),
                }
            }
        }
    }  else {
        println!("Starting {}", f_path.to_str().unwrap());
        db_hash = Some(("Adding", compute_hash(&f_path)));
    }

    let mut file_db = args.file_db.lock().unwrap();

    if let Some((add_str, hash)) = db_hash {
        file_db.add_file(f, &hash);
        println!("{add_str} {f} - {hash}!");
    }

    if file_db.num_changes() > 10 {
        file_db.save(&args.db_file);
    }
}

fn main() {
    let args = Args::parse();

    let mut files = BTreeSet::new();

    let base_path = std::env::current_dir().unwrap().canonicalize().unwrap();

    println!("Parsing {}", base_path.to_str().unwrap());

    for folder in args.folders.iter() {
        for f in find_files_in_directory(&base_path.join(folder), &args) {
            let rel_path = f.strip_prefix(&base_path).unwrap().to_path_buf();
            let p1 = RelativePathBuf::from_path(&rel_path).unwrap();
            files.insert(p1);
        }
    }

    let files = files;

    let targs = ThreadArgs::new(&args, &base_path, files.iter().cloned().collect());

    println!("Running with {} threads", args.processes);

    if args.processes == 0 {
        for f in files.iter() {
            process_file(&targs, f);
        }
    } else {
        let mut threads = Vec::new();

        for _ in 0..args.processes {
            let largs = targs.clone();
            let thread = std::thread::spawn(move || {
                loop {
                    let val = largs.input_queue.lock().unwrap().pop_front();

                    if let Some(f) = val {
                        process_file(&largs, &f);
                    } else {
                        break;
                    }
                }
            });
            threads.push(thread);
        }

        for t in threads {
            t.join().unwrap();
        }
    }

    let mut file_db = targs.file_db.lock().unwrap();

    if args.remove_old_entries {
        file_db.truncate_to_existing(&files);
    }

    if file_db.has_changes() {
        file_db.save(&args.db_file);
    }

    if *targs.fail_due_to_difference.lock().unwrap() {
        std::process::exit(1);
    }
}
