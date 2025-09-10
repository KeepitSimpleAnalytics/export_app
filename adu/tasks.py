#!/usr/bin/env python3
"""
Celery tasks for ADU Export Application
Asynchronous job processing tasks with enhanced logging and SQLite queue
"""

import time
import traceback
from celery import current_task
from adu.celery_config import celery_app
from adu.worker import process_data
from adu.enhanced_logger import logger
from adu.sqlite_writer import get_sqlite_writer

@celery_app.task(bind=True, name='adu.tasks.execute_export_job')
def execute_export_job(self, job_id, config):
    """
    Asynchronous task to execute database export job
    
    Args:
        job_id (str): Unique job identifier
        config (dict): Job configuration including database connection details
        
    Returns:
        dict: Job execution results
    """
    task_id = self.request.id
    logger.info(f"Starting export job {job_id} (Celery task: {task_id})")
    
    # Get SQLite writer for efficient database operations
    sqlite_writer = get_sqlite_writer()
    
    # Update job status to 'running'
    try:
        sqlite_writer.job_update(
            job_id=job_id,
            status='running',
            celery_task_id=task_id,
            start_time=time.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        logger.info("Job status updated to 'running'")
        
    except Exception as e:
        logger.error(f"Failed to update job status to running: {str(e)}")
        # Continue execution - this is not critical
    
    # Update task progress
    self.update_state(
        state='PROGRESS',
        meta={
            'job_id': job_id,
            'status': 'starting',
            'message': 'Initializing export job...'
        }
    )
    
    try:
        # Execute the actual export process
        logger.info("Executing database export process")
        
        # Log configuration summary
        tables_count = len(config.get('tables', []))
        db_info = f"{config.get('db_type', 'unknown')}://{config.get('db_host', 'unknown')}:{config.get('db_port', 'unknown')}"
        logger.info(f"Processing {tables_count} tables from {db_info}")
        
        # Update progress
        self.update_state(
            state='PROGRESS',
            meta={
                'job_id': job_id,
                'status': 'processing',
                'message': 'Processing database export...'
            }
        )
        
        # Call the enhanced worker processing function
        result = process_data(job_id, config)
        
        # Job completion is now handled by the worker's SQLite queue system
        logger.info("Export process completed successfully")
        
        # Return success result
        return {
            'job_id': job_id,
            'status': 'completed',
            'message': 'Export job completed successfully',
            'result': result if result else 'Export completed'
        }
        
    except Exception as exc:
        # Log the full exception with enhanced logging
        error_message = str(exc)
        tb = traceback.format_exc()
        logger.error(f"Export job failed: {error_message}")
        
        # Use SQLite writer to log the failure
        try:
            sqlite_writer.job_failed(job_id, error_message)
            sqlite_writer.log_error(job_id, error_message, tb, str(config))
        except Exception as e:
            logger.error(f"Failed to log job failure to database: {str(e)}")
        
        # Update task state to failure
        self.update_state(
            state='FAILURE',
            meta={
                'job_id': job_id,
                'status': 'failed',
                'message': f'Export job failed: {str(exc)}',
                'error': str(exc)
            }
        )
        
        # Re-raise the exception so Celery marks the task as failed
        raise exc

@celery_app.task(name='adu.tasks.get_job_status')
def get_job_status(job_id):
    """
    Get the current status of a job using SQLite writer queue
    
    Args:
        job_id (str): Job identifier
        
    Returns:
        dict: Job status information
    """
    try:
        sqlite_writer = get_sqlite_writer()
        
        query = """
            SELECT status, start_time, end_time, error_message, celery_task_id,
                   tables_total, tables_completed, tables_failed, progress_percent
            FROM jobs WHERE job_id = ?
        """
        
        result = sqlite_writer.query(query, (job_id,), fetchone=True, timeout=5.0)
        
        if result:
            return {
                'job_id': job_id,
                'status': result['status'],
                'start_time': result['start_time'],
                'end_time': result['end_time'],
                'error_message': result['error_message'],
                'celery_task_id': result['celery_task_id'],
                'tables_total': result['tables_total'],
                'tables_completed': result['tables_completed'],
                'tables_failed': result['tables_failed'],
                'progress_percent': result['progress_percent']
            }
        else:
            return {
                'job_id': job_id,
                'status': 'not_found',
                'error': 'Job not found'
            }
            
    except Exception as e:
        logger.error(f"Failed to get status for job {job_id}: {str(e)}")
        return {
            'job_id': job_id,
            'status': 'error',
            'error': str(e)
        }

@celery_app.task(name='adu.tasks.cancel_job')
def cancel_job(job_id):
    """
    Cancel a running job using SQLite writer queue
    
    Args:
        job_id (str): Job identifier
        
    Returns:
        dict: Cancellation result
    """
    try:
        sqlite_writer = get_sqlite_writer()
        
        # Get current job status and celery task ID
        query = "SELECT celery_task_id, status FROM jobs WHERE job_id = ?"
        result = sqlite_writer.query(query, (job_id,), fetchone=True, timeout=5.0)
        
        if not result:
            return {
                'job_id': job_id,
                'status': 'error',
                'message': 'Job not found'
            }
        
        celery_task_id = result['celery_task_id']
        current_status = result['status']
        
        if current_status in ['completed', 'failed', 'cancelled']:
            return {
                'job_id': job_id,
                'status': 'error',
                'message': f'Job is already {current_status} and cannot be cancelled'
            }
        
        # Update job status to cancelled using SQLite writer
        sqlite_writer.job_update(
            job_id=job_id,
            status='cancelled',
            end_time=time.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        # Revoke the Celery task if it has a task ID
        if celery_task_id:
            celery_app.control.revoke(celery_task_id, terminate=True)
            logger.info(f"Revoked Celery task {celery_task_id}")
        
        logger.info("Job cancelled successfully")
        
        return {
            'job_id': job_id,
            'status': 'cancelled',
            'message': 'Job cancelled successfully'
        }
        
    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {str(e)}")
        return {
            'job_id': job_id,
            'status': 'error',
            'error': str(e)
        }
