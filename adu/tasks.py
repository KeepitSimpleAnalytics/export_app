#!/usr/bin/env python3
"""
Celery tasks for ADU Export Application
Asynchronous job processing tasks
"""

import logging
import time
import traceback
from celery import current_task
from adu.celery_config import celery_app
from adu.worker import process_data
from adu.database import get_db_connection

# Configure logging for tasks
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    logger.info(f"Starting export job {job_id} (task: {task_id})")
    
    # Update job status to 'running'
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "UPDATE jobs SET status = ?, celery_task_id = ?, start_time = ? WHERE job_id = ?",
            ('running', task_id, time.strftime('%Y-%m-%d %H:%M:%S'), job_id)
        )
        conn.commit()
        conn.close()
        
        logger.info(f"Job {job_id} status updated to 'running'")
        
    except Exception as e:
        logger.error(f"Failed to update job {job_id} status to running: {str(e)}")
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
        logger.info(f"Executing export process for job {job_id}")
        
        # Update progress
        self.update_state(
            state='PROGRESS',
            meta={
                'job_id': job_id,
                'status': 'processing',
                'message': 'Processing database export...'
            }
        )
        
        # Call the main processing function from worker.py
        result = process_data(job_id, config)
        
        # Update job status to 'completed'
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "UPDATE jobs SET status = ?, end_time = ? WHERE job_id = ?",
                ('completed', time.strftime('%Y-%m-%d %H:%M:%S'), job_id)
            )
            conn.commit()
            conn.close()
            
            logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to update job {job_id} status to completed: {str(e)}")
        
        # Return success result
        return {
            'job_id': job_id,
            'status': 'completed',
            'message': 'Export job completed successfully',
            'result': result if result else 'Export completed'
        }
        
    except Exception as exc:
        # Log the full exception traceback
        logger.error(f"Export job {job_id} failed with exception: {str(exc)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Update job status to 'failed'
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "UPDATE jobs SET status = ?, end_time = ?, error_message = ? WHERE job_id = ?",
                ('failed', time.strftime('%Y-%m-%d %H:%M:%S'), str(exc), job_id)
            )
            conn.commit()
            conn.close()
            
            logger.info(f"Job {job_id} status updated to 'failed'")
            
        except Exception as e:
            logger.error(f"Failed to update job {job_id} status to failed: {str(e)}")
        
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
    Get the current status of a job
    
    Args:
        job_id (str): Job identifier
        
    Returns:
        dict: Job status information
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT status, start_time, end_time, error_message, celery_task_id FROM jobs WHERE job_id = ?",
            (job_id,)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            status, start_time, end_time, error_message, celery_task_id = result
            return {
                'job_id': job_id,
                'status': status,
                'start_time': start_time,
                'end_time': end_time,
                'error_message': error_message,
                'celery_task_id': celery_task_id
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
    Cancel a running job
    
    Args:
        job_id (str): Job identifier
        
    Returns:
        dict: Cancellation result
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the celery task ID
        cursor.execute(
            "SELECT celery_task_id, status FROM jobs WHERE job_id = ?",
            (job_id,)
        )
        
        result = cursor.fetchone()
        
        if not result:
            return {
                'job_id': job_id,
                'status': 'error',
                'message': 'Job not found'
            }
        
        celery_task_id, current_status = result
        
        if current_status in ['completed', 'failed', 'cancelled']:
            return {
                'job_id': job_id,
                'status': 'error',
                'message': f'Job is already {current_status} and cannot be cancelled'
            }
        
        # Update job status to cancelled
        cursor.execute(
            "UPDATE jobs SET status = ?, end_time = ? WHERE job_id = ?",
            ('cancelled', time.strftime('%Y-%m-%d %H:%M:%S'), job_id)
        )
        conn.commit()
        conn.close()
        
        # Revoke the Celery task if it has a task ID
        if celery_task_id:
            celery_app.control.revoke(celery_task_id, terminate=True)
            logger.info(f"Revoked Celery task {celery_task_id} for job {job_id}")
        
        logger.info(f"Job {job_id} cancelled successfully")
        
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
