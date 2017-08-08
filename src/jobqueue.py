#!/usr/bin/env python2

import sys
import os
import time
import errno
import argparse
import sqlite3
import subprocess
import json
import signal


dirname = os.path.join( "=>replace_me<=", "jobqueue")

class Sqlite:
    def __init__(self):
        self.open()

    def open(self):
        self.conn = sqlite3.connect(os.path.join(dirname, 'jobqueue.db'))
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def execute(self, stmt, params = None):
        if params:
            self.cur.execute(stmt, params)
        else:
            self.cur.execute(stmt)

    def fetchall(self):
        return self.cur.fetchall()

    def fetchone(self):
        return self.cur.fetchone()

    def create_table(self):
        try:
            self.cur.execute("""create table jobqueue(
            id integer primary key autoincrement,
            pid integer,
            dir text,
            cmd text,
            status text,
            name text,
            jobtype text
            )""")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

def error_exit(msg, exit_code = 1):
    sys.stderr.write("[ERROR]: {}\n".format( msg ))
    sys.exit( exit_code )
        

def is_running(pid):
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
    return True

def execute_command(cmd, jobname, jobid, working_dir):
    print "Start executing job [{}:{}]:".format(jobid, jobname)
    print "  * command: [{}]".format(str(cmd))
    print "  * working directory: [{}]".format( working_dir )
    ofile = os.path.join(working_dir, "{}.o{}".format(jobname, jobid))
    with open(ofile, "w") as fout:
        fout.write("CMD: {}\n".format(str(cmd)))
        try:
            process = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=fout, cwd=working_dir)
            sql = Sqlite()
            sql.execute("update jobqueue set status = 'running', pid = ? where id = ?", (process.pid, jobid))
            sql.commit()
            sql.close()
            process.wait()
        except KeyboardInterrupt:
            process.kill()
            print "job [{}:{}] killed: [{}]".format(jobid, jobname, str(cmd))
        except OSError as e:
            process.kill()
            print "job [{}:{}] failed with an OSError:".format(jobid, jobname)
            print e
        except ValueError as e:
            process.kill()
            print "job [{}:{}] failed with an ValueError:".format(jobid, jobname)
            print e
        finally:
            sql = Sqlite()
            sql.execute("delete from jobqueue where id=?", (jobid, ))
            sql.commit()
            sql.close()
            with open(os.path.join(dirname, "trashes.txt"), "a") as fout:
                fout.write("{}\n".format( os.path.abspath( ofile ) ))

        ret_code = process.returncode
        if ret_code == 0:
            print "job done!"
        else:
            print "job failed with return code [{}]".format( ret_code )
    print "================================================================================"
    print

def execute_wait(pid, jobname, jobid, working_dir):
    print "Start executing job [{}:{}]:".format(jobid, jobname)
    print "  * command: [wait {}]".format(pid)
    print "  * working directory: [{}]".format( working_dir )
    ofile = os.path.join(working_dir, "{}.o{}".format(jobname, jobid))
    with open(ofile, "w") as fout:
        fout.write("CMD: wait {}\n".format(pid))
        try:
            while is_running(pid):
                time.sleep(.25)
            sql = Sqlite()
            sql.execute("update jobqueue set status = 'running', pid = ? where id = ?", (os.getpid(), jobid))
            sql.commit()
            sql.close()
            print "job done"
        except KeyboardInterrupt:
            print "job [{}: {}] killed: [wait {}]".format(jobid, jobname, pid)
        finally:
            sql = Sqlite()
            sql.execute("delete from jobqueue where id = ?", (jobid, ))
            sql.commit()
            sql.close()
            with open(os.path.join(dirname, "trashes.txt"), "a") as fout:
                fout.write("{}\n".format(os.path.abspath( ofile )))
    print "================================================================================"
    print

def jobqueue_execute( sql ):
    sql.execute("select count(*) from jobqueue where status='running'")
    if sql.fetchone()[0] > 0:
        sql.close()
        return

    sql.execute("select * from jobqueue where status='waiting' limit 1")
    res = sql.fetchone()
    sql.close()
    if res == None:
        return
    
    while True:
        if res[-1] == 'normal':
            execute_command( json.loads(res[3]), res[5], res[0], res[2] )
        else:
            execute_wait( int(json.loads(res[3])), res[5], res[0], res[2] )
        sql = Sqlite()
        sql.execute("select * from jobqueue where status='waiting' limit 1")
        res = sql.fetchone()
        sql.close()
        if res == None:
            break
    

def jobqueue_add(args):
    sql = Sqlite()
    sql.execute("insert into jobqueue values(NULL, NULL, ?, ?, 'waiting', ?, 'normal')", (args.directory, json.dumps(args.cmd), args.name))
    sql.commit()
    jobqueue_execute( sql )


def jobqueue_list(args):
    sql = Sqlite()
    if args.status == None:
        sql.execute("select * from jobqueue")
    else:
        sql.execute("select * from jobqueue where status=?", (args.status, ))
    if args.id_only:
        for row in sql.fetchall():
            print row[0]
    else:
        for row in sql.fetchall():
            print "{}: [{}:{}]  [{}]  [{}] {} {}".format(row[0], row[1], row[5], str(json.loads(row[3])), row[2], row[4], row[6])
    sql.close()

def jobqueue_delete(args):
    sql = Sqlite()
    sql.execute("delete from jobqueue where id = ? and status <> 'running'", (args.ID, ))
    sql.execute("select pid, status, jobtype from jobqueue where id = ?", (args.ID, ))
    res = sql.fetchone()
    if res:
        if args.force:
            if res[2] =='normal':
                os.kill( int(res[0]), signal.SIGKILL )
            else:
                os.kill( int(res[0]), signal.SIGINT )
            sql.execute("delete from jobqueue where id = ?", (args.ID, ))
        else:
            print "The job is running and cannot be deleted"
    sql.commit()
    sql.close()
        
def jobqueue_clear(args):
    sql = Sqlite()
    sql.execute("delete from jobqueue where status <> 'running'")
    sql.execute("select pid, status, jobtype, id from jobqueue")
    pid = None
    if args.force:
        for row in sql.fetchall():
            if row[2] == 'normal':
                os.kill( int(row[0]), signal.SIGKILL )
            else:
                os.kill( int(row[0]), signal.SIGINT )
        sql.execute("delete from jobqueue")
    else:
        print "Warning: there are probably still jobs running, please check them!"
    sql.commit()
    sql.close()

def jobqueue_create(args):
    sql = Sqlite()
    sql.create_table()
    sql.close()

def jobqueue_recreate(args):
    sql = Sqlite()
    sql.execute("drop table jobqueue")
    sql.create_table()
    sql.close()

def remove_file(fname):
    fname = fname.strip()
    if fname == "": return
    try:
        os.remove(fname)
    except OSError as e:
        if e.errno != errno.ENOENT:
            print "[ERROR]: Cannot remove file [{}]:".format( fname ), e

def jobqueue_rmtrash(args):
    trash_file = os.path.join(dirname, "trashes.txt")
    try:
        with open(trash_file, "r") as fin:
            for line in fin:
                remove_file( line )
    except IOError as e:
        if e.errno == errno.ENOENT:
            return
        else:
            raise
    remove_file( trash_file )

def jobqueue_wait( args ):
    sql = Sqlite()
    sql.execute("insert into jobqueue values(NULL, NULL, ?, ?, 'waiting', ?, 'wait')", (args.directory, json.dumps(args.pid), args.name))
    sql.commit()
    jobqueue_execute( sql )

def parse_args():
    parser = argparse.ArgumentParser(description = "A sequential job scheduler")
    subparsers = parser.add_subparsers(title="subcommand", dest="subcmd")

    parser_add = subparsers.add_parser("add", help="Add a new job")
    parser_add.add_argument("-d", "--directory", default=os.getcwd(), help="Working directory (default $PWD)")
    parser_add.add_argument("cmd", nargs=argparse.REMAINDER, help="Command")
    parser_add.add_argument("-n", "--name", default="jobqueue", help="Set jobname")

    parser_list = subparsers.add_parser("list", help="List the jobs")
    parser_list.add_argument("-s", "--status", choices=["waiting", "running"], help="Specify a status to list")
    parser_list.add_argument("-i", "--id-only", action='store_true', help="Only list job IDs")

    parser_delete = subparsers.add_parser("delete", help="Delete a job")
    parser_delete.add_argument("ID", type=int, help="Job ID")
    parser_delete.add_argument("-f", "--force", action="store_true", help="Force remove the job even if it's running")

    parser_clear = subparsers.add_parser("clear", help="Clear all the jobs")
    parser_clear.add_argument("-f", "--force", action="store_true", help="Force to clear all the jobs even if the job is running")

    parser_create = subparsers.add_parser("create", help="Create table for database")
    parser_recreate = subparsers.add_parser("recreate", help="Delete the original table and create a new one")
    parser_clean = subparsers.add_parser("rmtrash", help="clean all the output files")

    parser_wait = subparsers.add_parser("wait", help="Wait for a PID")
    parser_wait.add_argument("pid", type=int, help="PID")
    parser_wait.add_argument("-d", "--directory", default=os.getcwd(), help="Working directory (default $PWD)")
    parser_wait.add_argument("-n", "--name", default="jobqueue", help="Set jobname")

    return parser.parse_args()


def main():
    args = parse_args()
    subcommands = {"add":       jobqueue_add,
                   "list":      jobqueue_list,
                   "delete":    jobqueue_delete,
                   "clear":     jobqueue_clear,
                   "create":    jobqueue_create,
                   "recreate":  jobqueue_recreate,
                   "rmtrash":   jobqueue_rmtrash,
                   "wait":      jobqueue_wait
                }
    try:
        subcommands[ args.subcmd ]( args )
    except KeyError:
        error_exit("Unknown subcommand {}".format( args.subcmd ))


if __name__ == '__main__':
    main()
