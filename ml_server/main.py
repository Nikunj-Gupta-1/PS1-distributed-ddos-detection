"""
Main server application combining FastAPI and gRPC for DDoS detection.
This file runs both REST and gRPC servers simultaneously.
"""

import asyncio
import logging
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import grpc
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

# Import our unified service
from serve import ml_service, FlowData, PredictionResult

# Import gRPC generated files (will be generated from prediction.proto)
try:
    import prediction_pb2
    import prediction_pb2_grpc
except ImportError:
    print("gRPC files not found. Run: python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. prediction.proto")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for FastAPI
class FlowDataRequest(BaseModel):
    """Request model for FastAPI endpoint"""
    request_id: str = Field(..., description="Unique request identifier")
    src_ip: str = Field(..., description="Source IP address")
    dst_ip: str = Field(..., description="Destination IP address") 
    timestamp: int = Field(..., description="Timestamp in microseconds")
    protocol: int = Field(..., description="IP protocol number")
    flow_duration: float = Field(..., description="Flow duration in microseconds")
    total_fwd_packets: int = Field(..., description="Total forward packets")
    total_backward_packets: int = Field(..., description="Total backward packets")
    fwd_packet_length_max: float = Field(..., description="Maximum forward packet length")
    fwd_packet_length_min: float = Field(..., description="Minimum forward packet length")
    fwd_packet_length_mean: float = Field(..., description="Mean forward packet length")
    packet_length_mean: float = Field(..., description="Mean packet length")
    packet_length_std: float = Field(..., description="Standard deviation of packet length")
    flow_bytes_per_second: float = Field(..., description="Flow bytes per second")
    flow_packets_per_second: float = Field(..., description="Flow packets per second")
    flow_iat_mean: float = Field(..., description="Mean inter-arrival time")
    flow_iat_std: float = Field(..., description="Standard deviation of IAT")
    flow_iat_max: float = Field(..., description="Maximum inter-arrival time")
    flow_iat_min: float = Field(..., description="Minimum inter-arrival time")
    fwd_iat_total: float = Field(..., description="Total forward inter-arrival time")
    fwd_iat_mean: float = Field(..., description="Mean forward inter-arrival time")
    fwd_iat_std: float = Field(..., description="Standard deviation of forward IAT")
    fwd_iat_max: float = Field(..., description="Maximum forward inter-arrival time")
    fwd_iat_min: float = Field(..., description="Minimum forward inter-arrival time")
    bwd_iat_total: float = Field(..., description="Total backward inter-arrival time")
    bwd_iat_mean: float = Field(..., description="Mean backward inter-arrival time")
    bwd_iat_std: float = Field(..., description="Standard deviation of backward IAT")
    bwd_iat_max: float = Field(..., description="Maximum backward inter-arrival time")
    bwd_iat_min: float = Field(..., description="Minimum backward inter-arrival time")
    fwd_psh_flags: float = Field(..., description="Forward PSH flag count")
    bwd_psh_flags: float = Field(..., description="Backward PSH flag count")
    fwd_urg_flags: float = Field(..., description="Forward URG flag count")

class PredictionResponse(BaseModel):
    """Response model for FastAPI endpoint"""
    prediction: int = Field(..., description="Prediction: 0=Benign, 1=Attack")
    confidence: float = Field(..., description="Confidence score (0.0-1.0)")
    request_id: str = Field(..., description="Original request ID")
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")

class BatchPredictionRequest(BaseModel):
    """Batch request model for FastAPI endpoint"""
    flows: List[FlowDataRequest] = Field(..., description="List of flow data")

class BatchPredictionResponse(BaseModel):
    """Batch response model for FastAPI endpoint"""
    predictions: List[PredictionResponse] = Field(..., description="List of predictions")

class HealthResponse(BaseModel):
    """Health check response model"""
    is_healthy: bool = Field(..., description="Service health status")
    model_status: str = Field(..., description="Model loading status")
    version: str = Field(..., description="Service version")
    uptime_seconds: int = Field(..., description="Service uptime in seconds")
    feature_count: int = Field(..., description="Number of features expected")

# Global variables for server management
grpc_server = None
should_stop = False

def convert_request_to_flow_data(request: FlowDataRequest) -> FlowData:
    """Convert FastAPI request to FlowData object"""
    return FlowData(
        request_id=request.request_id,
        src_ip=request.src_ip,
        dst_ip=request.dst_ip,
        timestamp=request.timestamp,
        protocol=request.protocol,
        flow_duration=request.flow_duration,
        total_fwd_packets=request.total_fwd_packets,
        total_backward_packets=request.total_backward_packets,
        fwd_packet_length_max=request.fwd_packet_length_max,
        fwd_packet_length_min=request.fwd_packet_length_min,
        fwd_packet_length_mean=request.fwd_packet_length_mean,
        packet_length_mean=request.packet_length_mean,
        packet_length_std=request.packet_length_std,
        flow_bytes_per_second=request.flow_bytes_per_second,
        flow_packets_per_second=request.flow_packets_per_second,
        flow_iat_mean=request.flow_iat_mean,
        flow_iat_std=request.flow_iat_std,
        flow_iat_max=request.flow_iat_max,
        flow_iat_min=request.flow_iat_min,
        fwd_iat_total=request.fwd_iat_total,
        fwd_iat_mean=request.fwd_iat_mean,
        fwd_iat_std=request.fwd_iat_std,
        fwd_iat_max=request.fwd_iat_max,
        fwd_iat_min=request.fwd_iat_min,
        bwd_iat_total=request.bwd_iat_total,
        bwd_iat_mean=request.bwd_iat_mean,
        bwd_iat_std=request.bwd_iat_std,
        bwd_iat_max=request.bwd_iat_max,
        bwd_iat_min=request.bwd_iat_min,
        fwd_psh_flags=request.fwd_psh_flags,
        bwd_psh_flags=request.bwd_psh_flags,
        fwd_urg_flags=request.fwd_urg_flags
    )

def convert_grpc_to_flow_data(request: prediction_pb2.PredictionRequest) -> FlowData:
    """Convert gRPC request to FlowData object"""
    return FlowData(
        request_id=request.request_id,
        src_ip=request.src_ip,
        dst_ip=request.dst_ip,
        timestamp=request.timestamp,
        protocol=request.protocol,
        flow_duration=request.flow_duration,
        total_fwd_packets=request.total_fwd_packets,
        total_backward_packets=request.total_backward_packets,
        fwd_packet_length_max=request.fwd_packet_length_max,
        fwd_packet_length_min=request.fwd_packet_length_min,
        fwd_packet_length_mean=request.fwd_packet_length_mean,
        packet_length_mean=request.packet_length_mean,
        packet_length_std=request.packet_length_std,
        flow_bytes_per_second=request.flow_bytes_per_second,
        flow_packets_per_second=request.flow_packets_per_second,
        flow_iat_mean=request.flow_iat_mean,
        flow_iat_std=request.flow_iat_std,
        flow_iat_max=request.flow_iat_max,
        flow_iat_min=request.flow_iat_min,
        fwd_iat_total=request.fwd_iat_total,
        fwd_iat_mean=request.fwd_iat_mean,
        fwd_iat_std=request.fwd_iat_std,
        fwd_iat_max=request.fwd_iat_max,
        fwd_iat_min=request.fwd_iat_min,
        bwd_iat_total=request.bwd_iat_total,
        bwd_iat_mean=request.bwd_iat_mean,
        bwd_iat_std=request.bwd_iat_std,
        bwd_iat_max=request.bwd_iat_max,
        bwd_iat_min=request.bwd_iat_min,
        fwd_psh_flags=request.fwd_psh_flags,
        bwd_psh_flags=request.bwd_psh_flags,
        fwd_urg_flags=request.fwd_urg_flags
    )

# gRPC Service Implementation
class DDoSDetectionServicer(prediction_pb2_grpc.DDoSDetectionServiceServicer):
    """gRPC service implementation"""
    
    def Predict(self, request, context):
        """Handle single prediction requests"""
        try:
            # Convert gRPC request to FlowData
            flow_data = convert_grpc_to_flow_data(request)
            
            # Make prediction
            result = ml_service.predict_single(flow_data)
            
            # Convert result to gRPC response
            response = prediction_pb2.PredictionResponse(
                prediction=result.prediction,
                confidence=result.confidence,
                request_id=result.request_id,
                processing_time_ms=result.processing_time_ms
            )
            
            return response
            
        except Exception as e:
            logger.error(f"gRPC prediction error: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Prediction failed: {str(e)}")
            return prediction_pb2.PredictionResponse()
    
    def PredictBatch(self, request, context):
        """Handle batch prediction requests"""
        try:
            # Convert gRPC requests to FlowData objects
            flow_data_list = []
            for req in request.requests:
                flow_data = convert_grpc_to_flow_data(req)
                flow_data_list.append(flow_data)
            
            # Make batch prediction
            results = ml_service.predict_batch(flow_data_list)
            
            # Convert results to gRPC response
            responses = []
            for result in results:
                response = prediction_pb2.PredictionResponse(
                    prediction=result.prediction,
                    confidence=result.confidence,
                    request_id=result.request_id,
                    processing_time_ms=result.processing_time_ms
                )
                responses.append(response)
            
            return prediction_pb2.BatchPredictionResponse(responses=responses)
            
        except Exception as e:
            logger.error(f"gRPC batch prediction error: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Batch prediction failed: {str(e)}")
            return prediction_pb2.BatchPredictionResponse()
    
    def HealthCheck(self, request, context):
        """Handle health check requests"""
        try:
            health_status = ml_service.get_health_status()
            
            response = prediction_pb2.HealthResponse(
                is_healthy=health_status['is_healthy'],
                model_status=health_status['model_status'],
                version=health_status['version'],
                uptime_seconds=health_status['uptime_seconds']
            )
            
            return response
            
        except Exception as e:
            logger.error(f"gRPC health check error: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Health check failed: {str(e)}")
            return prediction_pb2.HealthResponse()

# FastAPI Application
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan management"""
    # Startup
    logger.info("Starting FastAPI application...")
    
    # Start gRPC server in background
    grpc_thread = threading.Thread(target=start_grpc_server, daemon=True)
    grpc_thread.start()
    
    yield
    
    # Shutdown
    logger.info("Shutting down FastAPI application...")
    global should_stop
    should_stop = True
    
    if grpc_server:
        grpc_server.stop(grace=5)

# Create FastAPI app
app = FastAPI(
    title="DDoS Detection ML API",
    description="Machine Learning API for DDoS attack detection using 28-feature network flow analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# FastAPI Endpoints
@app.get("/", tags=["General"])
async def root():
    """Root endpoint"""
    return {
        "message": "DDoS Detection ML API",
        "version": "1.0.0",
        "endpoints": {
            "predict": "/predict",
            "batch_predict": "/predict/batch",
            "health": "/health",
            "docs": "/docs"
        }
    }

@app.post("/predict", response_model=PredictionResponse, tags=["Prediction"])
async def predict(request: FlowDataRequest):
    """Make a single prediction"""
    try:
        # Validate request data
        if request.flow_duration <= 0:
            raise HTTPException(status_code=400, detail="flow_duration must be positive")
        if request.total_fwd_packets < 0 or request.total_backward_packets < 0:
            raise HTTPException(status_code=400, detail="packet counts cannot be negative")
        
        # Convert request to FlowData
        flow_data = convert_request_to_flow_data(request)
        
        # Make prediction
        result = ml_service.predict_single(flow_data)
        
        # Convert result to response
        response = PredictionResponse(
            prediction=result.prediction,
            confidence=result.confidence,
            request_id=result.request_id,
            processing_time_ms=result.processing_time_ms
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"FastAPI prediction error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

@app.post("/predict/batch", response_model=BatchPredictionResponse, tags=["Prediction"])
async def predict_batch(request: BatchPredictionRequest):
    """Make batch predictions"""
    try:
        # Convert requests to FlowData objects
        flow_data_list = []
        for flow_request in request.flows:
            flow_data = convert_request_to_flow_data(flow_request)
            flow_data_list.append(flow_data)
        
        # Make batch prediction
        results = ml_service.predict_batch(flow_data_list)
        
        # Convert results to response
        predictions = []
        for result in results:
            prediction = PredictionResponse(
                prediction=result.prediction,
                confidence=result.confidence,
                request_id=result.request_id,
                processing_time_ms=result.processing_time_ms
            )
            predictions.append(prediction)
        
        return BatchPredictionResponse(predictions=predictions)
        
    except Exception as e:
        logger.error(f"FastAPI batch prediction error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {str(e)}")

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    try:
        health_status = ml_service.get_health_status()
        
        return HealthResponse(
            is_healthy=health_status['is_healthy'],
            model_status=health_status['model_status'],
            version=health_status['version'],
            uptime_seconds=health_status['uptime_seconds'],
            feature_count=health_status['feature_count']
        )
        
    except Exception as e:
        logger.error(f"FastAPI health check error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

def start_grpc_server():
    """Start gRPC server in a separate thread"""
    global grpc_server
    
    try:
        # Create gRPC server
        grpc_server = grpc.server(ThreadPoolExecutor(max_workers=10))
        
        # Add servicer to server
        prediction_pb2_grpc.add_DDoSDetectionServiceServicer_to_server(
            DDoSDetectionServicer(), grpc_server
        )
        
        # Bind to port
        listen_addr = '[::]:50051'
        grpc_server.add_insecure_port(listen_addr)
        
        # Start server
        grpc_server.start()
        logger.info(f"gRPC server started on {listen_addr}")
        
        # Keep server running
        while not should_stop:
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"gRPC server error: {str(e)}")
    
    finally:
        if grpc_server:
            grpc_server.stop(grace=5)
            logger.info("gRPC server stopped")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down...")
    global should_stop
    should_stop = True
    
    if grpc_server:
        grpc_server.stop(grace=5)
    
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start FastAPI server
    logger.info("Starting DDoS Detection ML API Server...")
    logger.info("FastAPI server will start on http://0.0.0.0:8000")
    logger.info("gRPC server will start on [::]:50051")
    logger.info("Interactive API docs available at http://0.0.0.0:8000/docs")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True
    )