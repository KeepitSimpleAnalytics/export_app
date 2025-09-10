#!/usr/bin/env python3
"""
Celery configuration for ADU Export Application
Asynchronous task processing setup
"""

import os
from celery import Celery

# Celery configuration
class CeleryConfig:
    # Broker settings
    broker_url = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    result_backend = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
    
    # Task settings
    task_serializer = 'json'
    accept_content = ['json']
    result_serializer = 'json'
    # Use system timezone instead of UTC for consistency with timestamps
    timezone = None  # Uses system timezone
    enable_utc = False
    
    # Task routing
    task_routes = {
        'adu.tasks.*': {'queue': 'export_jobs'},
    }
    
    # Worker settings
    worker_prefetch_multiplier = 1  # Only take one task at a time
    task_acks_late = True  # Acknowledge task only after completion
    worker_max_tasks_per_child = 10  # Restart worker after 10 tasks (prevent memory leaks)
    worker_max_memory_per_child = 8 * 1024 * 1024  # Restart worker at 8GB memory (KB units)
    
    # Task result settings
    result_expires = 3600  # Results expire after 1 hour
    task_ignore_result = False
    
    # Task execution settings
    task_soft_time_limit = 3600  # 1 hour soft limit
    task_time_limit = 7200  # 2 hour hard limit
    
    # Monitoring
    worker_send_task_events = True
    task_send_sent_event = True

def create_celery_app(app_name=__name__):
    """Create and configure Celery app"""
    celery = Celery(app_name)
    celery.config_from_object(CeleryConfig)
    
    # Update task base name
    class ContextTask(celery.Task):
        """Make celery tasks work with Flask app context."""
        def __call__(self, *args, **kwargs):
            return self.run(*args, **kwargs)
    
    celery.Task = ContextTask
    return celery

# Create the Celery instance
celery_app = create_celery_app('adu_export')
