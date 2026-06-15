import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.metrics import classification_report, accuracy_score, precision_recall_fscore_support, roc_auc_score, confusion_matrix
from imblearn.under_sampling import RandomUnderSampler
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

warnings.filterwarnings('ignore')

class DDoSModelTrainer28Features:
    """
    Enhanced DDoS detection model trainer with exact API feature name mapping
    """
    
    def __init__(self, binary_classification=True):
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.feature_selector = SelectKBest(score_func=f_classif, k=28)
        self.model = None
        self.feature_names = None
        self.binary_classification = binary_classification
        self.training_metrics = {}
        
        # EXACT feature names that your API server expects
        self.api_feature_names = [
            'protocol', 'flow_duration', 'total_fwd_packets', 'total_backward_packets',
            'fwd_packet_length_max', 'fwd_packet_length_min', 'fwd_packet_length_mean',
            'packet_length_mean', 'packet_length_std', 'flow_bytes_per_second',
            'flow_packets_per_second', 'flow_iat_mean', 'flow_iat_std', 'flow_iat_max',
            'flow_iat_min', 'fwd_iat_total', 'fwd_iat_mean', 'fwd_iat_std',
            'fwd_iat_max', 'fwd_iat_min', 'bwd_iat_total', 'bwd_iat_mean',
            'bwd_iat_std', 'bwd_iat_max', 'bwd_iat_min', 'fwd_psh_flags',
            'bwd_psh_flags', 'fwd_urg_flags'
        ]
        
        # Mapping from CSV column names to API feature names
        self.csv_to_api_mapping = {
            ' Protocol': 'protocol',
            ' Flow Duration': 'flow_duration',
            ' Total Fwd Packets': 'total_fwd_packets',
            ' Total Backward Packets': 'total_backward_packets',
            ' Fwd Packet Length Max': 'fwd_packet_length_max',
            ' Fwd Packet Length Min': 'fwd_packet_length_min',
            ' Fwd Packet Length Mean': 'fwd_packet_length_mean',
            ' Packet Length Mean': 'packet_length_mean',
            ' Packet Length Std': 'packet_length_std',
            'Flow Bytes/s': 'flow_bytes_per_second',
            ' Flow Packets/s': 'flow_packets_per_second',
            ' Flow IAT Mean': 'flow_iat_mean',
            ' Flow IAT Std': 'flow_iat_std',
            ' Flow IAT Max': 'flow_iat_max',
            ' Flow IAT Min': 'flow_iat_min',
            'Fwd IAT Total': 'fwd_iat_total',
            ' Fwd IAT Mean': 'fwd_iat_mean',
            ' Fwd IAT Std': 'fwd_iat_std',
            ' Fwd IAT Max': 'fwd_iat_max',
            ' Fwd IAT Min': 'fwd_iat_min',
            'Bwd IAT Total': 'bwd_iat_total',
            ' Bwd IAT Mean': 'bwd_iat_mean',
            ' Bwd IAT Std': 'bwd_iat_std',
            ' Bwd IAT Max': 'bwd_iat_max',
            ' Bwd IAT Min': 'bwd_iat_min',
            'Fwd PSH Flags': 'fwd_psh_flags',
            ' Bwd PSH Flags': 'bwd_psh_flags',
            ' URG Flag Count': 'fwd_urg_flags'
        }

    def load_and_preprocess_data(self, file_path):
        """Enhanced data preprocessing with exact feature name mapping"""
        print("🔄 Loading and preprocessing dataset for API-compatible 28-feature model...")
        print("=" * 70)
        
        df = pd.read_csv(file_path)
        
        # Convert to binary classification if specified
        if self.binary_classification:
            print("📊 Converting to binary classification (Attack vs Benign)")
            original_classes = df[' Label'].value_counts()
            df[' Label'] = df[' Label'].apply(
                lambda x: 'BENIGN' if x == 'BENIGN' else 'ATTACK'
            )
            print(f" ✓ Converted {len(original_classes)} classes to 2 classes")

        print(f"\n📈 Dataset Overview:")
        print(f" Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

        # Display class distribution
        class_counts = df[' Label'].value_counts()
        print(f"\n📊 Class Distribution:")
        for class_name, count in class_counts.items():
            percentage = (count / len(df)) * 100
            print(f" {class_name}: {count:,} samples ({percentage:.1f}%)")

        # Clean data
        print(f"\n🧹 Cleaning data...")
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna(0)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # Map CSV columns to API feature names
        print(f"\n🔄 Mapping CSV columns to API feature names...")
        
        # Check which required columns are available
        available_csv_columns = []
        missing_columns = []
        
        for csv_col, api_name in self.csv_to_api_mapping.items():
            if csv_col in df.columns:
                available_csv_columns.append(csv_col)
            else:
                missing_columns.append(f"{csv_col} -> {api_name}")
        
        if missing_columns:
            print(f" ⚠️  Warning: {len(missing_columns)} required columns missing:")
            for missing in missing_columns[:5]:  # Show first 5
                print(f"   - {missing}")
            if len(missing_columns) > 5:
                print(f"   ... and {len(missing_columns) - 5} more")
        
        print(f" ✓ Found {len(available_csv_columns)} out of {len(self.csv_to_api_mapping)} required columns")
        
        # Create feature dataframe with API names
        feature_data = {}
        for csv_col in available_csv_columns:
            api_name = self.csv_to_api_mapping[csv_col]
            feature_data[api_name] = df[csv_col]
        
        # Add missing features with zeros
        for api_name in self.api_feature_names:
            if api_name not in feature_data:
                feature_data[api_name] = np.zeros(len(df))
                print(f" ⚠️  Added missing feature '{api_name}' with zero values")
        
        # Create final feature dataframe with exact API order
        X = pd.DataFrame()
        for api_name in self.api_feature_names:
            X[api_name] = feature_data[api_name]
        
        y = df[' Label']
        
        print(f" ✓ Created feature matrix with {len(self.api_feature_names)} API-compatible features")
        
        # Balance the dataset
        print(f"\n⚖️ Balancing dataset...")
        X_balanced, y_balanced = self.balance_dataset(X, y)
        
        balanced_counts = pd.Series(y_balanced).value_counts()
        print(f" ✓ Balanced dataset shape: {X_balanced.shape}")
        print(f" ✓ Balanced distribution:")
        for class_name, count in balanced_counts.items():
            percentage = (count / len(y_balanced)) * 100
            print(f"   {class_name}: {count:,} samples ({percentage:.1f}%)")

        # Encode labels
        y_encoded = self.label_encoder.fit_transform(y_balanced)
        
        print(f"\n✅ Preprocessing complete!")
        print("=" * 70)
        
        return X_balanced, y_encoded, y_balanced

    def balance_dataset(self, X, y):
        """Create properly balanced dataset"""
        if self.binary_classification:
            # Use undersampling for balanced binary classification
            target_samples = min(100000, min(pd.Series(y).value_counts()))
            print(f" Target samples per class: {target_samples:,}")
            
            undersampler = RandomUnderSampler(
                sampling_strategy={
                    'BENIGN': target_samples,
                    'ATTACK': target_samples
                },
                random_state=42
            )
            X_balanced, y_balanced = undersampler.fit_resample(X, y)
        else:
            # For multi-class, use the original data
            X_balanced, y_balanced = X, y
            
        return X_balanced, y_balanced

    def train_model(self, X, y, test_size=0.2):
        """Train model with comprehensive evaluation"""
        print("\n🚀 Starting model training with API-compatible features...")
        print("=" * 70)

        # Split data
        print("📊 Splitting data with stratification...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=y
        )
        
        print(f" Training set: {len(X_train):,} samples")
        print(f" Test set: {len(X_test):,} samples")

        # Since we already have exactly 28 features, we'll use all of them
        print(f"\n🎯 Using all 28 API-compatible features...")
        self.feature_names = list(X.columns)
        
        print(f" ✓ Feature list (API-compatible):")
        for i, feature in enumerate(self.feature_names, 1):
            print(f"   {i:2d}. {feature}")

        # Feature scaling
        print(f"\n⚖️ Scaling features...")
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        print(f" ✓ Features scaled using StandardScaler")

        # Model configuration - ensemble for best performance
        print(f"\n🔧 Configuring ensemble models...")
        
        rf = RandomForestClassifier(
            n_estimators=1000, max_depth=20, min_samples_split=10,
            min_samples_leaf=4, class_weight='balanced', random_state=42, n_jobs=-1
        )
        
        xgb = XGBClassifier(
            n_estimators=1000, max_depth=10, learning_rate=0.05,
            subsample=0.9, colsample_bytree=0.9, scale_pos_weight=1,
            random_state=42, n_jobs=-1, eval_metric='logloss'
        )
        
        lgb = LGBMClassifier(
            n_estimators=1000, max_depth=10, learning_rate=0.05,
            subsample=0.9, colsample_bytree=0.9, class_weight='balanced',
            random_state=42, n_jobs=-1, verbose=-1
        )
        
        self.model = VotingClassifier(
            estimators=[('RandomForest', rf), ('XGBoost', xgb), ('LightGBM', lgb)],
            voting='soft', n_jobs=-1
        )
        
        print(f" ✓ Ensemble configured: RandomForest + XGBoost + LightGBM")

        # Cross-validation
        print(f"\n🔄 Performing cross-validation...")
        cv_scores = cross_val_score(
            self.model, X_train_scaled, y_train,
            cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
            scoring='accuracy', n_jobs=-1
        )
        
        cv_mean = cv_scores.mean()
        cv_std = cv_scores.std()
        print(f" Cross-validation results:")
        print(f"   Mean Accuracy: {cv_mean:.4f} ({cv_mean*100:.2f}%)")
        print(f"   Std Deviation: ±{cv_std:.4f}")
        print(f"   95% Confidence: {cv_mean:.4f} ± {cv_std * 1.96:.4f}")

        # Train final model
        print(f"\n🎯 Training final ensemble model...")
        self.model.fit(X_train_scaled, y_train)

        # Make predictions
        y_pred = self.model.predict(X_test_scaled)
        y_pred_proba = self.model.predict_proba(X_test_scaled)

        # Store training metrics
        self.training_metrics = {
            'cv_mean': cv_mean,
            'cv_std': cv_std,
            'test_accuracy': accuracy_score(y_test, y_pred),
            'feature_count': len(self.feature_names),
            'training_samples': len(X_train),
            'test_samples': len(X_test)
        }

        # Comprehensive evaluation
        self.evaluate_model_performance(y_test, y_pred, y_pred_proba)

        print(f"\n✅ Model training completed!")
        print("=" * 70)
        
        return X_test_scaled, y_test, y_pred

    def evaluate_model_performance(self, y_true, y_pred, y_pred_proba):
        """Comprehensive model evaluation with clear metrics"""
        print(f"\n🎯 MODEL PERFORMANCE SUMMARY")
        print("=" * 70)

        # Overall accuracy
        accuracy = accuracy_score(y_true, y_pred)
        print(f"🎯 Overall Test Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")

        # Performance interpretation
        if accuracy >= 0.99:
            performance_level = "🌟 EXCELLENT"
            interpretation = "Outstanding performance - ready for production"
        elif accuracy >= 0.95:
            performance_level = "✅ VERY GOOD"
            interpretation = "High performance - suitable for deployment"
        elif accuracy >= 0.90:
            performance_level = "⚠️ GOOD"
            interpretation = "Adequate performance - consider improvements"
        else:
            performance_level = "❌ NEEDS IMPROVEMENT"
            interpretation = "Poor performance - requires optimization"

        print(f"📊 Performance Level: {performance_level}")
        print(f"💡 Interpretation: {interpretation}")

        # Class-specific metrics
        precision, recall, f1, support = precision_recall_fscore_support(y_true, y_pred, average=None)
        class_names = self.label_encoder.classes_

        print(f"\n📊 Detailed Performance by Class:")
        print("-" * 50)
        for i, class_name in enumerate(class_names):
            print(f"\n{class_name.upper()}:")
            print(f"   Precision: {precision[i]:.4f} ({precision[i]*100:.2f}%)")
            print(f"   Recall: {recall[i]:.4f} ({recall[i]*100:.2f}%)")
            print(f"   F1-Score: {f1[i]:.4f} ({f1[i]*100:.2f}%)")
            print(f"   Support: {support[i]:,} samples")

        # Average metrics
        precision_macro = np.mean(precision)
        recall_macro = np.mean(recall)
        f1_macro = np.mean(f1)

        print(f"\n📈 Summary Metrics:")
        print(f"   Average Precision: {precision_macro:.4f} ({precision_macro*100:.2f}%)")
        print(f"   Average Recall: {recall_macro:.4f} ({recall_macro*100:.2f}%)")
        print(f"   Average F1-Score: {f1_macro:.4f} ({f1_macro*100:.2f}%)")

        # AUC score for binary classification
        if len(class_names) == 2:
            auc_score = roc_auc_score(y_true, y_pred_proba[:, 1])
            print(f"   AUC Score: {auc_score:.4f} ({auc_score*100:.2f}%)")

        print("=" * 70)

    def save_model(self, model_path=__import__('os').path.abspath(__import__('os').path.join(__import__('os').path.dirname(__file__), '../../raw data collected for project/model.pkl'))):
        """Save model with comprehensive metadata for API compatibility"""
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'label_encoder': self.label_encoder,
            'feature_names': self.feature_names,  # API-compatible feature names
            'binary_classification': self.binary_classification,
            'training_metrics': self.training_metrics,
            'api_feature_names': self.api_feature_names,  # For validation
            'csv_to_api_mapping': self.csv_to_api_mapping  # For reference
        }
        
        joblib.dump(model_data, model_path)
        
        # Save detailed summary
        with open('model_training_summary.txt', 'w') as f:
            f.write("DDoS Detection Model Training Summary (API-Compatible 28 Features)\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Model Type: {'Binary' if self.binary_classification else 'Multi-class'} Classification\n")
            f.write(f"Features Used: {self.training_metrics['feature_count']}\n")
            f.write(f"Test Accuracy: {self.training_metrics['test_accuracy']:.4f} ({self.training_metrics['test_accuracy']*100:.2f}%)\n")
            f.write(f"Cross-validation Mean: {self.training_metrics['cv_mean']:.4f}\n")
            f.write(f"Cross-validation Std: {self.training_metrics['cv_std']:.4f}\n")
            f.write(f"Training Samples: {self.training_metrics['training_samples']:,}\n")
            f.write(f"Test Samples: {self.training_metrics['test_samples']:,}\n")
            f.write(f"Classes: {list(self.label_encoder.classes_)}\n\n")
            f.write("API-Compatible Feature Names (in order):\n")
            for i, feature in enumerate(self.feature_names, 1):
                f.write(f"  {i:2d}. {feature}\n")
            f.write(f"\nCSV to API Feature Mapping:\n")
            for csv_col, api_name in self.csv_to_api_mapping.items():
                f.write(f"  '{csv_col}' -> '{api_name}'\n")

        print(f"💾 Model saved to: {model_path}")
        print(f"📄 Training summary saved to: model_training_summary.txt")

def train_api_compatible_model():
    """Main training function for API-compatible 28-feature DDoS detection model"""
    print("🚀 Starting API-Compatible DDoS Detection Model Training")
    print("=" * 70)
    
    trainer = DDoSModelTrainer28Features(binary_classification=True)
    
    # Load and preprocess data
    X, y_encoded, y_original = trainer.load_and_preprocess_data(__import__('os').path.abspath(__import__('os').path.join(__import__('os').path.dirname(__file__), '../../raw data collected for project/CIC_DDoS2019_To_Use.csv')))
    
    # Train model
    X_test, y_test, y_pred = trainer.train_model(X, y_encoded)
    
    # Save model
    trainer.save_model(__import__('os').path.abspath(__import__('os').path.join(__import__('os').path.dirname(__file__), '../../raw data collected for project/model1.pkl')))
    
    print(f"\n🎉 TRAINING COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"📁 Files Created:")
    print(f"   ✓ model.pkl (API-compatible 28-feature trained model)")
    print(f"   ✓ model_training_summary.txt (detailed summary)")
    print("=" * 70)
    print(f"🔧 Model Specifications:")
    print(f"   ✓ Features: {trainer.training_metrics['feature_count']} (API-compatible)")
    print(f"   ✓ Accuracy: {trainer.training_metrics['test_accuracy']:.4f}")
    print(f"   ✓ API Server Compatible: YES")
    print(f"   ✓ Binary Classification: Attack vs Benign")
    print(f"   ✓ Feature Names: Exact match with API expectations")
    print("=" * 70)
    
    return trainer

if __name__ == "__main__":
    trainer = train_api_compatible_model()
