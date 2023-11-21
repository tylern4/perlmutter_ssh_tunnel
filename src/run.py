#!/usr/bin/env python3
import json
import typer
from typing import Optional
from sfapi_client.jobs import JobCommand, Job, JobSqueue
from sfapi_client.compute import Machine, Compute
from sfapi_client import Client
import time
from fabric import Connection
import logging
from pathlib import Path
from tqdm import tqdm
from authlib.jose import JsonWebKey
import os

app = typer.Typer()


def print_json(*args, **kwargs):
    if isinstance(args[0], list):
        print(json.dumps([j.dict() for j in args[0]], default=str))
    else:
        try:
            print(json.dumps(args[0].dict(), default=str))
        except Exception:
            print(json.dumps(args[0], default=str))


@app.callback()
def main(
    ctx: typer.Context,
    debug: bool = typer.Option(False, help="Print out the logging debug"),
    site: Optional[Machine] = typer.Option(
        "perlmutter", "-s", "--site", help="Site to use"
    ),
):
    if debug:
        logging.basicConfig(encoding="utf-8", level=logging.DEBUG)
    else:
        logging.basicConfig(encoding="utf-8", level=logging.ERROR)

    client_id = os.getenv("SFAPI_CLIENT_ID")
    client_secret = os.getenv("SFAPI_SECRET")
    if None not in [client_id, client_secret]:
        # Use explicit environment variables
        secret = JsonWebKey.import_key(json.loads(client_secret))
        client = Client(client_id=client_id, secret=secret)
    else:
        # Try to get it from a file
        client = Client()
    # Use the client to get a compute object
    compute = client.compute(site)
    ctx.obj = compute


@app.command()
def cat(
    ctx: typer.Context,
    path: str = typer.Option(None, "-p", "--path", help="Path at NERSC")
):
    compute: Compute = ctx.obj
    [ret] = compute.ls(path)
    with ret.open('r') as fl:
        print(fl.read())


@app.command()
def hostname(ctx: typer.Context):
    compute: Compute = ctx.obj
    ret = compute.run("hostname")
    print(ret)


@app.command()
def token(ctx: typer.Context):
    compute: Compute = ctx.obj
    print(compute.client.token)


@app.command()
def status(ctx: typer.Context):
    compute: Compute = ctx.obj
    print_json(compute)


@app.command()
def ls(
    ctx: typer.Context,
    path: str = typer.Option(None, "-p", "--path", help="Path at NERSC"),
):
    compute: Compute = ctx.obj
    if path is None:
        user = compute.client.user()
        path = f"/global/homes/{user.name[0]}/{user.name}/"
    ret = compute.ls(path)
    print_json(ret)


@app.command()
def jobs(
    ctx: typer.Context,
    user: str = typer.Option(None, "-u", "--user",
                             help="Username to get jobs for"),
    command: Optional[JobCommand] = typer.Option(
        "squeue", "-c", help="Command used to get job info"),
):
    compute: Compute = ctx.obj
    ret = compute.jobs(user=user, command=command)
    print_json(ret)


@app.command()
def job(
    ctx: typer.Context,
    jobid: str = typer.Option(None, "-j", "--jobid",
                              help="Job id to get info for"),
    command: Optional[JobCommand] = typer.Option(
        "sacct", "-c", help="Command used to get job info"
    ),
):
    compute: Compute = ctx.obj
    ret = compute.job(jobid=jobid, command=command)
    print_json(ret)


@app.command("submit")
def submit_job(
    ctx: typer.Context,
    file_path: str = typer.Option(
        ..., "--file", "-f",
        help="Path to local slurm submit file or path at NERSC"),
    port_number: int = typer.Option(
        None, '--port', '-p',
        help="Port number to open ssh connection to"),
):
    compute: Compute = ctx.obj
    # Read in local path
    if Path(file_path).exists():
        file_path = Path(file_path).open('r').read()

    try:
        # Submit the job at the path
        running_job = compute.submit_job(file_path)
        logging.debug(f"submitted {running_job.jobid}")
        # Wait for the job to start running
        running_job.running()
        logging.debug(running_job.nodelist)
    except Exception as err:
        logging.error(f"Error {type(err).__name__}: {err}")

    if port_number is not None:
        open_ssh_connection(running_job, port_number)


@app.command("scancel")
def cancel_job(
    ctx: typer.Context,
    jobid: int = typer.Option(..., "--jobid", "-j", help="jobid to cancel"),
):
    compute: Compute = ctx.obj
    # Get job object
    job = compute.job(jobid=jobid, command=JobCommand.sacct)
    # Cancel job
    ret = job.cancel()
    print_json(ret)


def time_to_sec(time_limt: str):
    # Convert slurm time to ints
    total_time = 0
    if time_limt == 'INVALID':
        return total_time
    if len(time_limt.split("-")) > 1:
        total_time = 3600 * int(time_limt.split("-")[0])

    time_limt = time_limt.split("-")[-1]

    for i, t in enumerate(time_limt.split(":")[::-1]):
        total_time += int(t) * (60**i)
    return total_time


def open_ssh_connection(
    running_job: JobSqueue,
    port_number: int
):
    proxy_jump = Connection('perlmutter.nersc.gov',
                            user=running_job.user)
    ssh_client = Connection(running_job.nodelist,
                            gateway=proxy_jump,
                            user=running_job.user)
    # Open the connection to the node
    ssh_client.open()

    # Open port back to localhost
    with ssh_client.forward_local(port_number):
        print(
            f"Running tunnel to http://localhost:{port_number}\nTo cancel job ^C\n")
        # While we're still connected just loop and wait to end
        while ssh_client.is_connected:
            try:
                running_job.update()
                print(running_job.time_left)
                logging.info(
                    f'Time left {time_to_sec(running_job.time_left)} seconds')
                time.sleep(10)
            except KeyboardInterrupt:
                logging.info("\nCanceling job with ^C")
                running_job.cancel()
                print(f"\nCanceled job {running_job.jobid}")
                break

    ssh_client.close()
    running_job.update()
    logging.info(running_job.state)


if __name__ == "__main__":
    app()
