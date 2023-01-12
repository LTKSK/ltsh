use crate::helper::DynError;
use nix::{
    libc,
    sys::{
        signal::{killpg, signal, SigHandler, Signal},
        wait::{waitpid, WaitPidFlag, WaitStatus},
    },
    unistd::{self, dup2, execvp, fork, pipe, setpgid, tcgetpgrp, tcsetpgrp, ForkResult, Pid},
};
use rustyline::{error::ReadlineError, Editor};
use signal_hook::{consts::*, iterator::Signals};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::CString,
    mem::replace,
    path::PathBuf,
    process::exit,
    sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender},
    thread,
};

// ラッパー。EINTRならリトライする
fn syscall<F, T>(f: F) -> Result<T, nix::Error>
where
    F: Fn() -> Result<T, nix::Error>,
{
    loop {
        match f() {
            Err(nix::Error::EINTR) => (),
            result => return result,
        }
    }
}

enum WorkerMsg {
    Signal(i32),
    Cmd(String), // コマンドの入力
}

enum ShellMsg {
    Continue(i32), // shellの読み込みを再開。値は最後の終了コード
    Quit(i32),     // shellを終了する。値は終了コード
}

#[derive(Debug)]
pub struct Shell {
    logfile: String,
}

impl Shell {
    pub fn new(logfile: &str) -> Self {
        Shell {
            logfile: logfile.to_string(),
        }
    }

    // main thread
    pub fn run(&self) -> Result<(), DynError> {
        // SIGTTOUを無視しないとSIGTSTPが送信されてしまうらしい
        unsafe { signal(Signal::SIGTTOU, SigHandler::SigIgn).unwrap() };

        // Editorで標準入力を読み込んでいる
        let mut r1 = Editor::<()>::new()?; //
                                           // 読み込んだらまずはヒストリファイルを読み込んでいる
        if let Err(e) = r1.load_history(&self.logfile) {
            eprintln!("ltsh: ヒストリファイルの読み込みに失敗: {e}");
        }

        // workerスレッドとshellスレッド用
        let (worker_tx, worker_rx) = channel();
        // 0を入れると同期的なチャンネルになるんだとか
        let (shell_tx, shell_rx) = sync_channel(0);
        spawn_sig_handler(worker_tx.clone())?;

        Worker::new().spawn(worker_rx, shell_tx);

        let exit_val; // 終了コード
        let mut prev = 0; // 直前のプロセスの終了コード
        loop {
            let face = if prev == 0 { '\u{1F642}' } else { '\u{1F480}' };
            match r1.readline(&format!("ltsh {face} %>")) {
                Ok(line) => {
                    let line_trimed = line.trim();
                    if line_trimed.is_empty() {
                        continue; // からの場合再読込
                    } else {
                        r1.add_history_entry(line_trimed);
                    }

                    // 読み込んだ1行をワーカーに送信
                    worker_tx.send(WorkerMsg::Cmd(line)).unwrap();
                    // ここで受信待ち。recvが待受する関数
                    match shell_rx.recv().unwrap() {
                        ShellMsg::Continue(n) => prev = n, // 読み込み再開
                        ShellMsg::Quit(n) => {
                            exit_val = n;
                            break;
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => eprintln!("ltsh: 終了は Ctrl-d"),
                Err(ReadlineError::Eof) => {
                    worker_tx.send(WorkerMsg::Cmd("exit".to_string())).unwrap();
                    match shell_rx.recv().unwrap() {
                        ShellMsg::Quit(n) => {
                            exit_val = n;
                            break;
                        }
                        _ => panic!("exit失敗"),
                    }
                }
                Err(e) => {
                    eprintln!("ltsh: 読み込みエラー\n{e}");
                    exit_val = 1;
                    break;
                }
            }
        }
        if let Err(e) = r1.save_history(&self.logfile) {
            eprintln!("ltsh: ヒストリファイルへの書き込みに失敗: {e}");
        }

        exit(exit_val);
    }
}

fn spawn_sig_handler(tx: Sender<WorkerMsg>) -> Result<(), DynError> {
    // SIGTSTP、SIGINTはC-c,C-zが押されたときに終了しないために受信。大事なのはSIGCHLD
    let mut signals = Signals::new(&[SIGINT, SIGTSTP, SIGCHLD])?;
    thread::spawn(move || {
        // ここでシグナルの受信を待ち受けていて、流れてきたらtxにそのままパスする
        for sig in signals.forever() {
            // 行き先はworker thread
            tx.send(WorkerMsg::Signal(sig)).unwrap();
        }
    });

    Ok(())
}

// Procから始まる以下の2つはジョブ管理用の型
#[derive(Debug, PartialEq, Eq, Clone)]
enum ProcState {
    Run,  //実行中
    Stop, // 停止中
}

#[derive(Debug, Clone)]
struct ProcInfo {
    state: ProcState, // 実行状態
    pgid: Pid,        // プロセルのグループID
}

#[derive(Debug)]
struct Worker {
    exit_val: i32,
    fg: Option<Pid>,                      // フォアグラウンドのプロセスグループID
    jobs: BTreeMap<usize, (Pid, String)>, // jobI -> (プロセスグループID,実行コマンド)
    pgid_to_pids: HashMap<Pid, (usize, HashSet<Pid>)>, // プロセスグループId -> (ジョブID, プロセスID)
    pid_to_info: HashMap<Pid, ProcInfo>,               // プロセスIDからプロセスグループIDへのマップ
    shell_pgid: Pid,                                   // シェルのプロセスグループID
}

impl Worker {
    fn new() -> Self {
        Worker {
            exit_val: 0,
            fg: None, // foregroundはshell
            jobs: BTreeMap::new(),
            pgid_to_pids: HashMap::new(),
            pid_to_info: HashMap::new(),
            // shellのプロセスグループIDを取得
            shell_pgid: tcgetpgrp(libc::STDIN_FILENO).unwrap(),
        }
    }

    fn run_exit(&mut self, args: &[&str], shell_tx: &SyncSender<ShellMsg>) -> bool {
        if !self.jobs.is_empty() {
            eprintln!("ジョブが実行中なので終了できません");
            self.exit_val = 1;
            shell_tx.send(ShellMsg::Continue(self.exit_val)).unwrap(); // シェルを再開
            return true;
        }

        let exit_val = if let Some(s) = args.get(1) {
            if let Ok(n) = (*s).parse::<i32>() {
                n
            } else {
                eprintln!("{s}は不正な引数です");
                self.exit_val = 1;
                // shellを再開
                shell_tx.send(ShellMsg::Continue(self.exit_val)).unwrap();
                return true;
            }
        } else {
            self.exit_val
        };

        shell_tx.send(ShellMsg::Quit(exit_val)).unwrap();
        true
    }

    fn run_fg(&mut self, args: &[&str], shell_tx: &SyncSender<ShellMsg>) -> bool {
        self.exit_val = 1;

        if args.len() < 2 {
            eprintln!("usage: fg 数字");
            shell_tx.send(ShellMsg::Continue(self.exit_val)).unwrap(); // シェルを再開
            return true;
        }

        if let Ok(n) = args[1].parse::<usize>() {
            if let Some((pgid, cmd)) = self.jobs.get(&n) {
                eprintln!("[{n}] 再開\t{cmd}");

                self.fg = Some(*pgid);
                // ここでpgidのプロセスグループをfdの紐づくセッションのフォアグラウンドにする
                tcsetpgrp(libc::STDIN_FILENO, *pgid).unwrap();

                // pgidに所属するジョブを再開
                killpg(*pgid, Signal::SIGCONT).unwrap();
                return true;
            }
        }

        // 失敗
        eprintln!("{}というジョブは見つかりませんでした", args[1]);
        shell_tx.send(ShellMsg::Continue(self.exit_val)).unwrap(); // シェルを再開
        true
    }

    fn build_in_cmd(&mut self, cmd: &[(&str, Vec<&str>)], shell_tx: &SyncSender<ShellMsg>) -> bool {
        if cmd.len() > 1 {
            return false; // 組み込みコマンドのパイプは非対応
        }

        match cmd[0].0 {
            "exit" => self.run_exit(&cmd[0].1, shell_tx),
            // "jobs" => self.run_jobs(&cmd[0].1, shell_tx),
            "fg" => self.run_fg(&cmd[0].1, shell_tx),
            // "cd" => self.run_cd(&cmd[0].1, shell_tx),
            _ => false,
        }
    }

    fn spawn_child(&mut self, line: &str, cmd: &[(&str, Vec<&str>)]) -> bool {
        assert_ne!(cmd.len(), 0); // cmdが空でないことを確認

        let job_id = if let Some(id) = self.get_new_job_id() {
            id
        } else {
            eprintln!("ltsh: 管理可能なジョブの最大数に到達");
            return false;
        };

        if cmd.len() > 2 {
            eprintln!("ltsh: 3つ以上のコマンドによるパイプはサポートしていません");
            return false;
        }

        let mut input = None; // 2つ目のプロセスのstdin
        let mut output = None; // 1つ目のプロセスのstdout
        if cmd.len() == 2 {
            let p = pipe().unwrap();
            input = Some(p.0);
            output = Some(p.1);
        }

        let cleanup_pipe = CleanUp {
            f: || {
                if let Some(fd) = input {
                    syscall(|| unistd::close(fd)).unwrap();
                }
                if let Some(fd) = output {
                    syscall(|| unistd::close(fd)).unwrap();
                }
            },
        };

        let pgid;
        // fork_execでプロセスを生成。Pid::from_raw(0)はでプロセスグループIDを取得している。0を渡すと生成したプロセスと同じプロセスグループIDになる
        // コマンドの標準入出力先も渡す必要がある
        match fork_exec(Pid::from_raw(0), cmd[0].0, &cmd[0].1, None, output) {
            Ok(child) => {
                pgid = child;
            }
            Err(e) => {
                eprintln!("ltsh: プロセス生成エラー: {e}");
                return false;
            }
        }

        let info = ProcInfo {
            state: ProcState::Run,
            pgid,
        };
        let mut pids = HashMap::new();
        pids.insert(pgid, info.clone()); //1つ目のプロセス情報

        if cmd.len() == 2 {
            match fork_exec(pgid, cmd[1].0, &cmd[1].1, input, None) {
                Ok(child) => {
                    pids.insert(child, info);
                }
                Err(e) => {
                    eprintln!("ltsh: プロセス生成エラー: {e}");
                    return false;
                }
            }
        }

        std::mem::drop(cleanup_pipe); // pipeをクローズ。これは自前でやる必要あり

        self.fg = Some(pgid);
        self.insert_job(job_id, pgid, pids, line);
        tcsetpgrp(libc::STDIN_FILENO, pgid).unwrap();

        true
    }

    fn spawn(mut self, worker_rx: Receiver<WorkerMsg>, shell_tx: SyncSender<ShellMsg>) {
        thread::spawn(move || {
            for msg in worker_rx.iter() {
                match msg {
                    WorkerMsg::Cmd(line) => {
                        match parse_cmd(&line) {
                            Ok(cmd) => {
                                // shellの内部コマンドを実行する
                                if self.build_in_cmd(&cmd, &shell_tx) {
                                    continue; // 組み込みコマンドだったらworker_rxから受信
                                }

                                if !self.spawn_child(&line, &cmd) {
                                    // プロセス生成失敗時はシェルから入力を再開する
                                    shell_tx.send(ShellMsg::Continue(self.exit_val)).unwrap();
                                }
                            }
                            Err(e) => {
                                eprintln!("ltsh: {e}");
                                shell_tx.send(ShellMsg::Continue(self.exit_val)).unwrap();
                            }
                        }
                    }
                    // SIGCHLDシグナルを受信したときは子プロセスの状態変化を管理する
                    WorkerMsg::Signal(SIGCHLD) => {
                        self.wait_child(&shell_tx);
                    }
                    _ => (), // 無視
                }
            }
        });
    }
}

type CmdResult<'a> = Result<Vec<(&'a str, Vec<&'a str>)>, DynError>;

fn parse_cmd(line: &str) -> CmdResult {
    let commands: Vec<&str> = line.split('|').collect();
    let result: Vec<(&str, Vec<&str>)> = commands
        .iter()
        .map(|cmd_and_args| {
            let cmd_and_args = cmd_and_args.split(' ').collect::<Vec<&str>>();
            let cmd = cmd_and_args[0];
            (cmd, cmd_and_args)
        })
        .collect();
    Ok(result)
}

struct CleanUp<F>
where
    F: Fn(),
{
    f: F,
}

impl<F> Drop for CleanUp<F>
where
    F: Fn(),
{
    fn drop(&mut self) {
        (self.f)()
    }
}
