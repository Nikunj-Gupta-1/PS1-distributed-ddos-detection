"""
Unified API handler for DDoS detection predictions.
Handles both REST and gRPC requests with shared ML logic.
"""

import joblib
import numpy as np
import pandas as pd
import logging
import time
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class FlowData:
    """Data class for network flow features"""
    request_id: str
    src_ip: str
    dst_ip: str
    timestamp: int
    protocol: int
    flow_duration: float
    total_fwd_packets: int
    total_backward_packets: int
    fwd_packet_length_max: float
    fwd_packet_length_min: float
    fwd_packet_length_mean: float
    packet_length_mean: float
    packet_length_std: float
    flow_bytes_per_second: float
    flow_packets_per_second: float
    flow_iat_mean: float
    flow_iat_std: float
    flow_iat_max: float
    flow_iat_min: float
    fwd_iat_total: float
    fwd_iat_mean: float
    fwd_iat_std: float
    fwd_iat_max: float
    fwd_iat_min: float
    bwd_iat_total: float
    bwd_iat_mean: float
    bwd_iat_std: float
    bwd_iat_max: float
    bwd_iat_min: float
    fwd_psh_flags: float
    bwd_psh_flags: float
    fwd_urg_flags: float

@dataclass
class PredictionResult:
    """Result of a prediction"""
    prediction: int
    confidence: float
    request_id: str
    processing_time_ms: int

class MLModelService:
    """Unified ML service for DDoS detection"""

    THRESHOLD = 0.9  # Detection threshold for malicious confidence

    def __init__(self, model_path: str = __import__('os').path.abspath(__import__('os').path.join(__import__('os').path.dirname(__file__), '../../raw data collected for project/model4.pkl'))):
        """Initialize the ML service with model loading"""
        self.model_path = model_path
        self.model = None
        self.scaler = None
        self.feature_names = None
        self.start_time = time.time()

        # Load model on initialization
        self._load_model()

    def _load_model(self) -> None:
        """Load the trained model and preprocessing components"""
        try:
            logger.info(f"Loading model from {self.model_path}")

            # Load the model file
            model_data = joblib.load(self.model_path)

            # Handle different model file formats
            if isinstance(model_data, dict):
                self.model = model_data.get('model')
                self.scaler = model_data.get('scaler')
                self.feature_names = model_data.get('feature_names', self._get_default_feature_names())
            else:
                # If it's just the model directly
                self.model = model_data
                self.scaler = None
                self.feature_names = self._get_default_feature_names()

            if self.model is None:
                raise ValueError("Model not found in the loaded file")

            logger.info("Model loaded successfully")
            logger.info(f"Model type: {type(self.model).__name__}")
            logger.info(f"Feature count: {len(self.feature_names)}")

        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise

    def _get_default_feature_names(self) -> List[str]:
        """Get default feature names for the 28-feature model"""
        return [
            'protocol', 'flow_duration', 'total_fwd_packets', 'total_backward_packets',
            'fwd_packet_length_max', 'fwd_packet_length_min', 'fwd_packet_length_mean',
            'packet_length_mean', 'packet_length_std', 'flow_bytes_per_second',
            'flow_packets_per_second', 'flow_iat_mean', 'flow_iat_std', 'flow_iat_max',
            'flow_iat_min', 'fwd_iat_total', 'fwd_iat_mean', 'fwd_iat_std',
            'fwd_iat_max', 'fwd_iat_min', 'bwd_iat_total', 'bwd_iat_mean',
            'bwd_iat_std', 'bwd_iat_max', 'bwd_iat_min', 'fwd_psh_flags',
            'bwd_psh_flags', 'fwd_urg_flags'
        ]

    def _extract_features(self, flow_data: FlowData) -> np.ndarray:
        """Extract numeric features from flow data"""
        # Extract the 28 numeric features in correct order
        features = [
            flow_data.protocol,
            flow_data.flow_duration,
            flow_data.total_fwd_packets,
            flow_data.total_backward_packets,
            flow_data.fwd_packet_length_max,
            flow_data.fwd_packet_length_min,
            flow_data.fwd_packet_length_mean,
            flow_data.packet_length_mean,
            flow_data.packet_length_std,
            flow_data.flow_bytes_per_second,
            flow_data.flow_packets_per_second,
            flow_data.flow_iat_mean,
            flow_data.flow_iat_std,
            flow_data.flow_iat_max,
            flow_data.flow_iat_min,
            flow_data.fwd_iat_total,
            flow_data.fwd_iat_mean,
            flow_data.fwd_iat_std,
            flow_data.fwd_iat_max,
            flow_data.fwd_iat_min,
            flow_data.bwd_iat_total,
            flow_data.bwd_iat_mean,
            flow_data.bwd_iat_std,
            flow_data.bwd_iat_max,
            flow_data.bwd_iat_min,
            flow_data.fwd_psh_flags,
            flow_data.bwd_psh_flags,
            flow_data.fwd_urg_flags
        ]

        # Convert to numpy array
        features_array = np.array(features, dtype=np.float64)

        # Handle invalid values
        features_array = np.nan_to_num(features_array, nan=0.0, posinf=0.0, neginf=0.0)

        return features_array.reshape(1, -1)

    def _preprocess_features(self, features: np.ndarray) -> np.ndarray:
        """Preprocess features using the loaded scaler"""
        if self.scaler is not None:
            try:
                features_scaled = self.scaler.transform(features)
                return features_scaled
            except Exception as e:
                logger.warning(f"Error scaling features: {str(e)}")
                return features
        else:
            return features

    def _apply_threshold(self, prediction_prob: np.ndarray, classes: List[str]) -> Tuple[int, float]:
        """Apply threshold to prediction probabilities and return binary prediction and confidence"""
        if len(prediction_prob) > 2:
            benign_prob = prediction_prob[0] if 'BENIGN' in str(classes[0]).upper() else 0.0
            attack_prob = 1.0 - benign_prob
            prediction = 1 if attack_prob > self.THRESHOLD else 0
            confidence = max(benign_prob, attack_prob)
        else:
            attack_prob = prediction_prob[1] if len(prediction_prob) > 1 else 0.0
            prediction = 1 if attack_prob > self.THRESHOLD else 0
            confidence = attack_prob if prediction == 1 else prediction_prob[0]
        return prediction, confidence

    def predict_single(self, flow_data: FlowData) -> PredictionResult:
        """Make a single prediction"""
        start_time = time.time()

        try:
            # Extract features
            features = self._extract_features(flow_data)

            # Preprocess features
            features_processed = self._preprocess_features(features)

            # Make prediction
            prediction_prob = self.model.predict_proba(features_processed)[0]
            prediction_raw = self.model.predict(features_processed)[0]

            # Apply threshold logic
            if hasattr(self.model, 'classes_'):
                binary_prediction, confidence = self._apply_threshold(prediction_prob, self.model.classes_)
            else:
                binary_prediction = 1 if prediction_raw > self.THRESHOLD else 0
                confidence = float(prediction_raw) if binary_prediction == 1 else float(1 - prediction_raw)

            processing_time = int((time.time() - start_time) * 1000)

            result = PredictionResult(
                prediction=binary_prediction,
                confidence=float(confidence),
                request_id=flow_data.request_id,
                processing_time_ms=processing_time
            )

            logger.info(f"Prediction completed - ID: {flow_data.request_id}, "
                        f"Result: {binary_prediction}, Confidence: {confidence:.3f}, "
                        f"Time: {processing_time}ms")

            return result

        except Exception as e:
            logger.error(f"Error making prediction: {str(e)}")
            processing_time = int((time.time() - start_time) * 1000)
            return PredictionResult(
                prediction=0,
                confidence=0.0,
                request_id=flow_data.request_id,
                processing_time_ms=processing_time
            )

    def predict_batch(self, flow_data_list: List[FlowData]) -> List[PredictionResult]:
        """Make batch predictions"""
        results = []
        start_time = time.time()

        try:
            # Extract features for all flows
            features_list = []
            for flow_data in flow_data_list:
                features = self._extract_features(flow_data)
                features_list.append(features[0])  # Remove the extra dimension

            # Stack features into a single array
            features_batch = np.vstack(features_list)

            # Preprocess features
            features_processed = self._preprocess_features(features_batch)

            # Make batch predictions
            predictions_raw = self.model.predict(features_processed)
            prediction_probs = self.model.predict_proba(features_processed)

            # Process each prediction
            for flow_data, prediction_raw, prob in zip(flow_data_list, predictions_raw, prediction_probs):
                if hasattr(self.model, 'classes_'):
                    binary_prediction, confidence = self._apply_threshold(prob, self.model.classes_)
                else:
                    binary_prediction = 1 if prediction_raw > self.THRESHOLD else 0
                    confidence = float(prediction_raw) if binary_prediction == 1 else float(1 - prediction_raw)

                result = PredictionResult(
                    prediction=binary_prediction,
                    confidence=float(confidence),
                    request_id=flow_data.request_id,
                    processing_time_ms=0  # To be updated after batch completes
                )
                results.append(result)

            # Set processing time for all results
            total_time = int((time.time() - start_time) * 1000)
            for result in results:
                result.processing_time_ms = total_time // len(results)

            logger.info(f"Batch prediction completed - {len(results)} predictions in {total_time}ms")

        except Exception as e:
            logger.error(f"Error making batch prediction: {str(e)}")
            # Return error results
            for flow_data in flow_data_list:
                result = PredictionResult(
                    prediction=0,
                    confidence=0.0,
                    request_id=flow_data.request_id,
                    processing_time_ms=0
                )
                results.append(result)

        return results

    def get_health_status(self) -> Dict:
        """Get health status of the service"""
        return {
            "is_healthy": self.model is not None,
            "model_status": "loaded" if self.model is not None else "not_loaded",
            "version": "1.0.0",
            "uptime_seconds": int(time.time() - self.start_time),
            "feature_count": len(self.feature_names) if self.feature_names else 0
        }


# Global service instance
ml_service = MLModelService()
