#!/usr/bin/env python3
"""
SLURM Job Preemption Monitor and Rollback Script

This script monitors SLURM job states and automatically rolls back preempted jobs 
from spot partitions to regular partitions. It validates preemption events by 
examining SLURM reason codes and partition constraints to prevent false positives.

Based on the pseudo-code from the paper:
1. Initialize job_status_map = {} and preemption_count = {}
2. While True:
    a. Fetch SLURM queue state: `squeue --format="%i,%T,%R,%P"`
    b. For each job: compare current vs. previous status
    c. Validate preemption assumption:
       - RUNNING -> PENDING + NodeFail reason + spot partition = preemption
       - Other transitions or reasons indicate scheduling constraints
    d. For validated preempted jobs:
       - Execute `scontrol update JobID=<id> Partition=regular`
       - Log rollback action and increment preemption count
    e. Update job_status_map and sleep for 60 seconds
"""

import subprocess
import time
import logging
import os
import sys
import signal
import atexit
from datetime import datetime
from typing import Dict, Tuple, Set
from dataclasses import dataclass
from pathlib import Path
from rich.console import Console
from rich.logging import RichHandler

from batch_processing.cmd.base import BaseCommand

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=Console(), rich_tracebacks=True)]
)

logger = logging.getLogger("monitor")


class Daemon:
    """Daemon class to run the monitor in the background"""
    
    def __init__(self, pidfile, logfile=None):
        self.pidfile = pidfile
        self.logfile = logfile or "/tmp/slurm-monitor.log"
        
    def daemonize(self):
        """Fork and run as daemon"""
        try:
            # First fork
            pid = os.fork()
            if pid > 0:
                sys.exit(0)  # Exit parent
        except OSError as e:
            logger.error(f"Fork #1 failed: {e}")
            sys.exit(1)
            
        # Decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)
        
        try:
            # Second fork
            pid = os.fork()
            if pid > 0:
                sys.exit(0)  # Exit second parent
        except OSError as e:
            logger.error(f"Fork #2 failed: {e}")
            sys.exit(1)
            
        # Redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        
        # Setup logging to file
        self._setup_file_logging()
        
        # Write pidfile
        atexit.register(self.delpid)
        with open(self.pidfile, 'w') as f:
            f.write(f"{os.getpid()}\n")
            
    def _setup_file_logging(self):
        """Setup logging to file for daemon"""
        # Remove existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        # Add file handler
        file_handler = logging.FileHandler(self.logfile)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        logging.root.addHandler(file_handler)
        logging.root.setLevel(logging.INFO)
        
    def delpid(self):
        """Remove PID file"""
        try:
            os.remove(self.pidfile)
        except FileNotFoundError:
            pass
            
    def start(self, target_func):
        """Start the daemon"""
        # Check if already running
        if self.is_running():
            logger.error("Daemon already running")
            return False
            
        logger.info("Starting SLURM monitor daemon...")
        self.daemonize()
        target_func()
        
    def stop(self):
        """Stop the daemon"""
        pid = self.get_pid()
        if not pid:
            logger.info("Daemon not running")
            return False
            
        try:
            os.kill(pid, signal.SIGTERM)
            # Wait for process to terminate
            for _ in range(30):  # Wait up to 30 seconds
                try:
                    os.kill(pid, 0)  # Check if process exists
                    time.sleep(1)
                except OSError:
                    break
            else:
                # Force kill if still running
                logger.warning("Force killing daemon")
                os.kill(pid, signal.SIGKILL)
                
            self.delpid()
            logger.info("Daemon stopped")
            return True
            
        except OSError as e:
            logger.error(f"Failed to stop daemon: {e}")
            return False
            
    def restart(self, target_func):
        """Restart the daemon"""
        self.stop()
        time.sleep(1)
        self.start(target_func)
        
    def is_running(self):
        """Check if daemon is running"""
        pid = self.get_pid()
        if not pid:
            return False
            
        try:
            os.kill(pid, 0)  # Check if process exists
            return True
        except OSError:
            return False
            
    def get_pid(self):
        """Get PID from pidfile"""
        try:
            with open(self.pidfile, 'r') as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None
            
    def status(self):
        """Get daemon status"""
        if self.is_running():
            pid = self.get_pid()
            logger.info(f"Daemon running (PID: {pid})")
            return True
        else:
            logger.info("Daemon not running")
            return False


@dataclass
class JobState:
    """Represents the state of a SLURM job"""
    job_id: str
    status: str
    reason: str
    partition: str
    timestamp: datetime


class SlurmJobMonitor:
    """Monitors SLURM jobs and handles preemption rollbacks"""
    
    def __init__(self, spot_partitions: Set[str] = None, regular_partition: str = "compute"):
        """
        Initialize the monitor
        
        Args:
            spot_partitions: Set of partition names for spot/preemptible nodes
            regular_partition: Name of the regular/on-premise partition
        """
        self.spot_partitions = spot_partitions or {"spot", "dask"}
        self.regular_partition = regular_partition
        self.job_status_map: Dict[str, JobState] = {}
        self.preemption_count: Dict[str, int] = {}
        
        logger.info(f"Initialized SLURM monitor")
        logger.info(f"Spot partitions: {self.spot_partitions}")
        logger.info(f"Regular partition: {self.regular_partition}")
    
    def fetch_slurm_queue_state(self) -> Dict[str, JobState]:
        """
        Fetch current SLURM queue state using squeue command
        
        Returns:
            Dictionary mapping job_id to JobState
        """
        try:
            cmd = ["squeue", "--format=%i,%T,%R,%P", "--noheader"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            current_jobs = {}
            timestamp = datetime.now()
            
            for line in result.stdout.strip().split('\n'):
                if not line.strip():
                    continue
                    
                parts = line.strip().split(',')
                if len(parts) >= 4:
                    job_id, status, reason, partition = parts[0], parts[1], parts[2], parts[3]
                    current_jobs[job_id] = JobState(
                        job_id=job_id,
                        status=status,
                        reason=reason,
                        partition=partition,
                        timestamp=timestamp
                    )
            
            return current_jobs
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to fetch SLURM queue state: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching queue state: {e}")
            return {}
    
    def validate_preemption(self, job_id: str, old_state: JobState, new_state: JobState) -> bool:
        """
        Validate whether a job state transition indicates preemption
        
        Args:
            job_id: Job ID
            old_state: Previous job state
            new_state: Current job state
            
        Returns:
            True if this appears to be a preemption, False otherwise
        """
        # Check for RUNNING -> PENDING transition
        if old_state.status != "RUNNING" or new_state.status != "PENDING":
            return False
        
        # Check if job was on a spot partition
        if old_state.partition not in self.spot_partitions:
            return False
        
        # Check for NodeFail reason (indicates node preemption)
        # Common SLURM reasons for preemption: NodeFail, NodeDown, NodeNotAvail
        preemption_reasons = {"NodeFail", "NodeDown", "NodeNotAvail", "NodeTerminated"}
        if new_state.reason not in preemption_reasons:
            logger.debug(f"Job {job_id}: Status change not due to preemption (reason: {new_state.reason})")
            return False
        
        logger.info(f"Job {job_id}: Validated preemption - {old_state.status} -> {new_state.status}, reason: {new_state.reason}")
        return True
    
    def rollback_job(self, job_id: str) -> bool:
        """
        Rollback a preempted job to the regular partition
        
        Args:
            job_id: Job ID to rollback
            
        Returns:
            True if rollback was successful, False otherwise
        """
        try:
            # Update job partition to regular
            cmd = ["scontrol", "update", f"JobID={job_id}", f"Partition={self.regular_partition}"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Increment preemption count
            self.preemption_count[job_id] = self.preemption_count.get(job_id, 0) + 1
            
            logger.info(f"Job {job_id}: Successfully rolled back to '{self.regular_partition}' partition "
                       f"(preemption #{self.preemption_count[job_id]})")
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Job {job_id}: Failed to rollback - {e}")
            return False
        except Exception as e:
            logger.error(f"Job {job_id}: Unexpected error during rollback - {e}")
            return False
    
    def cleanup_completed_jobs(self, current_jobs: Dict[str, JobState]):
        """
        Clean up tracking data for completed/cancelled jobs
        
        Args:
            current_jobs: Dictionary of currently active jobs
        """
        # Find jobs that are no longer in the queue
        tracked_jobs = set(self.job_status_map.keys())
        current_job_ids = set(current_jobs.keys())
        completed_jobs = tracked_jobs - current_job_ids
        
        # Remove completed jobs from tracking
        for job_id in completed_jobs:
            if job_id in self.job_status_map:
                old_status = self.job_status_map[job_id].status
                logger.debug(f"Job {job_id}: Removing from tracking (last status: {old_status})")
                del self.job_status_map[job_id]
            
            # Keep preemption counts for reporting, but could be cleaned up after some time
            # if job_id in self.preemption_count:
            #     del self.preemption_count[job_id]
    
    def monitor_jobs(self):
        """
        Main monitoring loop - continuously monitor and handle preemptions
        """
        logger.info("Starting SLURM job monitoring loop...")
        
        try:
            while True:
                current_jobs = self.fetch_slurm_queue_state()
                
                if not current_jobs:
                    logger.debug("No jobs found in queue or failed to fetch queue state")
                else:
                    logger.debug(f"Monitoring {len(current_jobs)} jobs")
                
                preemptions_detected = 0
                
                # Check each job for preemption
                for job_id, current_state in current_jobs.items():
                    if job_id in self.job_status_map:
                        # Job exists in our tracking - check for state changes
                        old_state = self.job_status_map[job_id]
                        
                        if self.validate_preemption(job_id, old_state, current_state):
                            if self.rollback_job(job_id):
                                preemptions_detected += 1
                    else:
                        # New job - add to tracking
                        logger.debug(f"Job {job_id}: Adding to tracking (status: {current_state.status}, "
                                   f"partition: {current_state.partition})")
                
                # Update job status map with current states
                self.job_status_map.update(current_jobs)
                
                # Clean up completed jobs from tracking
                self.cleanup_completed_jobs(current_jobs)
                
                # Log summary if preemptions were detected
                if preemptions_detected > 0:
                    total_preemptions = sum(self.preemption_count.values())
                    logger.info(f"Cycle complete: {preemptions_detected} new preemptions detected, "
                               f"{total_preemptions} total preemptions handled")
                
                # Sleep for 60 seconds before next check
                time.sleep(60)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error in monitoring loop: {e}")
            raise


class MonitorCommand(BaseCommand):
    """Command class for SLURM job monitoring and preemption rollback"""
    
    def __init__(self, args):
        super().__init__()
        self._args = args
        
        # Setup daemon configuration
        home_dir = Path.home()
        self.pidfile = home_dir / ".slurm-monitor.pid"
        self.logfile = home_dir / ".slurm-monitor.log"
        self.daemon = Daemon(str(self.pidfile), str(self.logfile))
    
    def _run_monitor(self):
        """Internal method to run the actual monitoring"""
        # Fixed partition configuration
        spot_partitions = ["spot", "dask"]
        regular_partition = "compute"
        
        logger.info("Starting SLURM preemption monitor...")
        logger.info(f"Monitoring spot partitions: {spot_partitions}")
        logger.info(f"Rolling back to partition: {regular_partition}")
        
        # Setup signal handling for graceful shutdown
        def signal_handler(signum, frame):
            logger.info("Received shutdown signal, stopping monitor...")
            sys.exit(0)
            
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        # Initialize and start monitor
        monitor = SlurmJobMonitor(
            spot_partitions=set(spot_partitions),
            regular_partition=regular_partition
        )
        
        try:
            monitor.monitor_jobs()
        except KeyboardInterrupt:
            logger.info("Monitor stopped by user")
        except Exception as e:
            logger.error(f"Monitor failed: {e}")
            raise
    
    def execute(self):
        """Execute the monitor command - always starts as daemon"""
        action = getattr(self._args, 'action', 'start')
        
        if action == 'start':
            if self.daemon.is_running():
                logger.info("SLURM monitor is already running")
                pid = self.daemon.get_pid()
                logger.info(f"PID: {pid}, Log file: {self.logfile}")
                return
                
            logger.info("Starting SLURM monitor daemon...")
            logger.info(f"PID file: {self.pidfile}")
            logger.info(f"Log file: {self.logfile}")
            
            self.daemon.start(self._run_monitor)
            
        elif action == 'stop':
            if self.daemon.stop():
                logger.info("SLURM monitor daemon stopped")
            else:
                logger.error("Failed to stop daemon or daemon not running")
                
        elif action == 'restart':
            logger.info("Restarting SLURM monitor daemon...")
            self.daemon.restart(self._run_monitor)
            
        elif action == 'status':
            self.daemon.status()
            
        else:
            logger.error(f"Unknown action: {action}")
            sys.exit(1)
