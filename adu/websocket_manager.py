#!/usr/bin/env python3
"""
WebSocket Manager for Real-Time Progress Updates
"""

import asyncio
import json
import logging
from typing import Set, Dict, Callable
from flask_socketio import SocketIO, emit, join_room, leave_room
from threading import Lock

class WebSocketManager:
    """Manages WebSocket connections for real-time updates"""
    
    def __init__(self, app=None):
        self.socketio = None
        self.active_connections: Dict[str, Set[str]] = {}  # job_id -> set of session_ids
        self._lock = Lock()
        
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize WebSocket with Flask app"""
        self.socketio = SocketIO(
            app,
            cors_allowed_origins="*",
            async_mode='threading',
            logger=True,
            engineio_logger=False
        )
        
        # Register event handlers
        self._register_handlers()
        
        logging.info("WebSocket manager initialized")
    
    def _register_handlers(self):
        """Register WebSocket event handlers"""
        
        @self.socketio.on('connect')
        def handle_connect():
            logging.info(f"Client connected: {self.socketio.request.sid}")
            emit('connected', {'status': 'Connected to ADU progress updates'})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            session_id = self.socketio.request.sid
            logging.info(f"Client disconnected: {session_id}")
            
            # Remove from all job rooms
            with self._lock:
                for job_id, sessions in self.active_connections.items():
                    if session_id in sessions:
                        sessions.remove(session_id)
                        leave_room(job_id)
                        
                # Clean up empty job rooms
                self.active_connections = {
                    job_id: sessions 
                    for job_id, sessions in self.active_connections.items() 
                    if sessions
                }
        
        @self.socketio.on('subscribe_job')
        def handle_subscribe_job(data):
            """Subscribe to updates for a specific job"""
            job_id = data.get('job_id')
            session_id = self.socketio.request.sid
            
            if job_id:
                join_room(job_id)
                
                with self._lock:
                    if job_id not in self.active_connections:
                        self.active_connections[job_id] = set()
                    self.active_connections[job_id].add(session_id)
                
                emit('subscribed', {'job_id': job_id, 'status': 'Subscribed to job updates'})
                logging.info(f"Client {session_id} subscribed to job {job_id}")
        
        @self.socketio.on('unsubscribe_job')
        def handle_unsubscribe_job(data):
            """Unsubscribe from job updates"""
            job_id = data.get('job_id')
            session_id = self.socketio.request.sid
            
            if job_id:
                leave_room(job_id)
                
                with self._lock:
                    if job_id in self.active_connections:
                        self.active_connections[job_id].discard(session_id)
                        if not self.active_connections[job_id]:
                            del self.active_connections[job_id]
                
                emit('unsubscribed', {'job_id': job_id, 'status': 'Unsubscribed from job updates'})
                logging.info(f"Client {session_id} unsubscribed from job {job_id}")
    
    def broadcast_job_progress(self, job_id: str, progress_data: Dict):
        """Broadcast progress update to all subscribers of a job"""
        if self.socketio and job_id in self.active_connections:
            self.socketio.emit(
                'job_progress', 
                progress_data, 
                room=job_id
            )
            logging.debug(f"Broadcasted progress for job {job_id} to {len(self.active_connections[job_id])} clients")
    
    def broadcast_job_status(self, job_id: str, status: str, message: str = None):
        """Broadcast job status change"""
        if self.socketio and job_id in self.active_connections:
            self.socketio.emit(
                'job_status', 
                {
                    'job_id': job_id,
                    'status': status,
                    'message': message,
                    'timestamp': asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else None
                }, 
                room=job_id
            )
    
    def get_active_job_subscribers(self) -> Dict[str, int]:
        """Get count of active subscribers per job"""
        with self._lock:
            return {job_id: len(sessions) for job_id, sessions in self.active_connections.items()}

# Global WebSocket manager instance
websocket_manager = WebSocketManager()