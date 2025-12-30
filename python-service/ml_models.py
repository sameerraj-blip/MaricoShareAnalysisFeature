"""Machine Learning Model Training Functions"""
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Literal, Union
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge, Lasso, ElasticNet, BayesianRidge
from sklearn.ensemble import (
    RandomForestRegressor, RandomForestClassifier, GradientBoostingRegressor, GradientBoostingClassifier,
    ExtraTreesRegressor, ExtraTreesClassifier
)
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier
from sklearn.svm import SVR, SVC
from sklearn.neighbors import KNeighborsRegressor, KNeighborsClassifier
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis, QuadraticDiscriminantAnalysis
from sklearn.neural_network import MLPRegressor, MLPClassifier
from sklearn.cluster import (
    KMeans, MiniBatchKMeans, AgglomerativeClustering, DBSCAN, MeanShift, SpectralClustering, Birch
)
from sklearn.decomposition import PCA, KernelPCA, FastICA, FactorAnalysis, TruncatedSVD, NMF
from sklearn.manifold import TSNE
from sklearn.ensemble import IsolationForest
from sklearn.covariance import EllipticEnvelope
from sklearn.metrics import (
    r2_score, mean_squared_error, mean_absolute_error,
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report, silhouette_score
)
import traceback

# Optional imports for advanced models
try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False

try:
    import catboost as cb
    CATBOOST_AVAILABLE = True
except ImportError:
    CATBOOST_AVAILABLE = False

try:
    from sklearn.gaussian_process import GaussianProcessRegressor
    from sklearn.gaussian_process.kernels import RBF, ConstantKernel as C
    GAUSSIAN_PROCESS_AVAILABLE = True
except ImportError:
    GAUSSIAN_PROCESS_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

try:
    from umap import UMAP
    UMAP_AVAILABLE = True
except ImportError:
    UMAP_AVAILABLE = False

try:
    from hdbscan import HDBSCAN
    HDBSCAN_AVAILABLE = True
except ImportError:
    HDBSCAN_AVAILABLE = False


def _prepare_data(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str]
) -> tuple:
    """
    Prepare data for model training.
    
    Returns:
        X (DataFrame): Feature matrix
        y (Series): Target variable
    """
    df = pd.DataFrame(data)
    
    if len(df) == 0:
        raise ValueError("Dataset is empty")
    
    # Check if columns exist
    missing_cols = [col for col in [target_variable] + features if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in data: {', '.join(missing_cols)}. Available columns: {', '.join(df.columns.tolist()[:10])}")
    
    # Extract features and target
    X = df[features].copy()
    y = df[target_variable].copy()
    
    # Check initial null counts for better error messages
    initial_rows = len(df)
    target_null_count = y.isna().sum()
    feature_null_counts = {col: X[col].isna().sum() for col in features}
    
    # Convert to numeric, coercing errors to NaN
    for col in features:
        if not pd.api.types.is_numeric_dtype(X[col]):
            X[col] = pd.to_numeric(X[col], errors='coerce')
    
    if not pd.api.types.is_numeric_dtype(y):
        y = pd.to_numeric(y, errors='coerce')
    
    # Check how many rows have valid target
    valid_target_mask = ~y.isna()
    valid_target_count = valid_target_mask.sum()
    
    if valid_target_count == 0:
        raise ValueError(
            f"Target variable '{target_variable}' has no valid numeric values. "
            f"All {initial_rows} rows have null or non-numeric values in the target variable."
        )
    
    # Remove rows with NaN in target
    X = X[valid_target_mask]
    y = y[valid_target_mask]
    
    # Check how many rows have at least one valid feature
    feature_valid_mask = X.notna().any(axis=1)
    valid_feature_count = feature_valid_mask.sum()
    
    if valid_feature_count == 0:
        raise ValueError(
            f"After removing rows with null target values, no rows have valid feature values. "
            f"Target variable '{target_variable}' had {valid_target_count} valid values, "
            f"but all features are null in those rows. "
            f"Feature null counts: {feature_null_counts}"
        )
    
    # Remove rows where all features are NaN
    X = X[feature_valid_mask]
    y = y[feature_valid_mask]
    
    # Fill remaining NaN in features with mean (only for columns that have at least one non-null value)
    for col in X.columns:
        if X[col].isna().any():
            col_mean = X[col].mean()
            if pd.isna(col_mean):
                # If mean is NaN, all values are NaN - this shouldn't happen after filtering, but handle it
                raise ValueError(f"Feature '{col}' has no valid numeric values after cleaning")
            X[col] = X[col].fillna(col_mean)
    
    if len(X) == 0:
        raise ValueError(
            f"No valid data rows after cleaning. "
            f"Initial rows: {initial_rows}, "
            f"Rows with valid target: {valid_target_count}, "
            f"Rows with valid features: {valid_feature_count}"
        )
    
    if len(X) < 2:
        raise ValueError(
            f"Need at least 2 data points to train a model, but only {len(X)} valid row(s) found after cleaning. "
            f"Initial rows: {initial_rows}, "
            f"Target nulls: {target_null_count}, "
            f"Feature nulls: {feature_null_counts}"
        )
    
    return X, y


def _determine_task_type(y: pd.Series) -> Literal["regression", "classification"]:
    """Determine if task is regression or classification based on target variable."""
    # If target is numeric and has many unique values, treat as regression
    if pd.api.types.is_numeric_dtype(y):
        unique_ratio = y.nunique() / len(y)
        # If more than 10% unique values, treat as regression
        if unique_ratio > 0.1:
            return "regression"
        # If binary (2 unique values), treat as classification
        elif y.nunique() == 2:
            return "classification"
        # If few unique values, treat as classification
        else:
            return "classification"
    else:
        return "classification"


def _calculate_regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """Calculate regression metrics."""
    return {
        "r2_score": float(r2_score(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "mse": float(mean_squared_error(y_true, y_pred))
    }


def _calculate_classification_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, Any]:
    """Calculate classification metrics."""
    accuracy = float(accuracy_score(y_true, y_pred))
    
    # For binary classification, calculate precision, recall, F1
    if len(np.unique(y_true)) == 2:
        precision = float(precision_score(y_true, y_pred, average='binary', zero_division=0))
        recall = float(recall_score(y_true, y_pred, average='binary', zero_division=0))
        f1 = float(f1_score(y_true, y_pred, average='binary', zero_division=0))
    else:
        # Multi-class
        precision = float(precision_score(y_true, y_pred, average='weighted', zero_division=0))
        recall = float(recall_score(y_true, y_pred, average='weighted', zero_division=0))
        f1 = float(f1_score(y_true, y_pred, average='weighted', zero_division=0))
    
    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1
    }


def train_linear_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a linear regression model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        # Coefficients
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        # Predictions on full dataset
        y_pred_full = model.predict(X)
        
        return {
            "model_type": "linear_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": None,  # Linear regression doesn't have feature importance
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training linear regression: {str(e)}")


def train_log_log_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    test_size: float = 0.2,
    random_state: int = 42,
    offset: float = 1.0
) -> Dict[str, Any]:
    """
    Train a log-log regression model.
    
    A log-log model applies log transformation to both the target variable
    and all feature variables before training a linear regression. This is useful
    for modeling multiplicative relationships and elasticity analysis.
    
    Args:
        data: List of dictionaries containing the data
        target_variable: Name of the target variable
        features: List of feature variable names
        test_size: Proportion of data to use for testing
        random_state: Random seed for reproducibility
        offset: Offset to add before log transformation (to handle zeros/negatives)
    
    Returns:
        Dictionary containing model results, metrics, and coefficients
    """
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Check for non-positive values that would cause issues with log transformation
        # For target variable
        if (y <= 0).any():
            negative_or_zero_count = (y <= 0).sum()
            raise ValueError(
                f"Log-log model requires all target values to be positive. "
                f"Found {negative_or_zero_count} non-positive values in '{target_variable}'. "
                f"Consider using an offset or filtering out non-positive values."
            )
        
        # For feature variables
        for feature in features:
            if (X[feature] <= 0).any():
                negative_or_zero_count = (X[feature] <= 0).sum()
                raise ValueError(
                    f"Log-log model requires all feature values to be positive. "
                    f"Found {negative_or_zero_count} non-positive values in feature '{feature}'. "
                    f"Consider using an offset or filtering out non-positive values."
                )
        
        # Apply log transformation to target
        y_log = np.log(y)
        
        # Apply log transformation to features
        X_log = X.copy()
        for feature in features:
            X_log[feature] = np.log(X_log[feature])
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_log, y_log, test_size=test_size, random_state=random_state
        )
        
        # Train model on log-transformed data
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        # Predictions (in log space)
        y_train_pred_log = model.predict(X_train)
        y_test_pred_log = model.predict(X_test)
        
        # Convert predictions back to original scale (exponential)
        y_train_pred = np.exp(y_train_pred_log)
        y_test_pred = np.exp(y_test_pred_log)
        
        # Convert actual values back to original scale for metrics
        y_train_actual = np.exp(y_train.values)
        y_test_actual = np.exp(y_test.values)
        
        # Metrics (calculated in original scale)
        train_metrics = _calculate_regression_metrics(y_train_actual, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test_actual, y_test_pred)
        
        # Cross-validation (in log space, then convert metrics)
        cv_scores = cross_val_score(model, X_log, y_log, cv=5, scoring='r2')
        
        # Coefficients (in log-log space)
        # Interpretation: a 1% change in feature X leads to a (coefficient)% change in target Y
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            },
            "interpretation": "Coefficients represent elasticities: a 1% change in a feature leads to a (coefficient)% change in the target variable"
        }
        
        # Predictions on full dataset (in original scale)
        y_pred_full_log = model.predict(X_log)
        y_pred_full = np.exp(y_pred_full_log)
        
        return {
            "model_type": "log_log_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": None,  # Linear regression doesn't have feature importance
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test),
            "transformation_applied": "log-log transformation applied to both target and features",
            "note": "Model coefficients represent elasticities. A coefficient of 0.5 means a 1% increase in the feature leads to a 0.5% increase in the target."
        }
    except Exception as e:
        raise ValueError(f"Error training log-log regression: {str(e)}")


def train_logistic_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a logistic regression model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Determine if classification is needed
        task_type = _determine_task_type(y)
        
        # Handle binary target conversion
        unique_values = sorted(y.unique())
        is_binary = len(unique_values) == 2
        
        if is_binary and set(unique_values).issubset({0, 1}):
            # Already binary (0/1), use as-is
            task_type = "classification"
        elif is_binary:
            # Binary but not 0/1, convert to 0/1
            y = y.map({unique_values[0]: 0, unique_values[1]: 1})
            task_type = "classification"
        elif task_type != "classification":
            # Convert continuous to binary using median threshold
            median = y.median()
            y = (y > median).astype(int)
            task_type = "classification"
        
        # Use stratify for binary classification to maintain class distribution
        use_stratify = len(y.unique()) == 2 and min(y.value_counts()) >= 2
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y if use_stratify else None
        )
        
        # Train model
        model = LogisticRegression(max_iter=1000, random_state=random_state)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='accuracy')
        
        # Coefficients - for logistic regression, coef_ has shape (n_classes, n_features)
        # For binary classification, we take the first row
        coef_array = model.coef_[0] if len(model.coef_.shape) > 1 else model.coef_
        coefficients = {
            "intercept": float(model.intercept_[0]) if len(model.intercept_) == 1 else model.intercept_.tolist(),
            "features": {
                feature: float(coef)
                for feature, coef in zip(features, coef_array)
            }
        }
        
        # Predictions on full dataset
        y_pred_full = model.predict(X)
        
        return {
            "model_type": "logistic_regression",
            "task_type": "classification",
            "target_variable": target_variable,
            "features": features,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_accuracy": float(cv_scores.mean()),
                    "std_accuracy": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training logistic regression: {str(e)}")


def train_ridge_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    alpha: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a ridge regression model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = Ridge(alpha=alpha, random_state=random_state)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        # Coefficients
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        # Predictions on full dataset
        y_pred_full = model.predict(X)
        
        return {
            "model_type": "ridge_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "alpha": alpha,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training ridge regression: {str(e)}")


def train_lasso_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    alpha: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a lasso regression model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = Lasso(alpha=alpha, random_state=random_state, max_iter=1000)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        # Coefficients
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        # Predictions on full dataset
        y_pred_full = model.predict(X)
        
        return {
            "model_type": "lasso_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "alpha": alpha,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training lasso regression: {str(e)}")


def train_random_forest(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    n_estimators: int = 100,
    max_depth: Optional[int] = None,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a random forest model (regression or classification)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Determine task type
        task_type = _determine_task_type(y)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = RandomForestRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state
            )
        else:
            model = RandomForestClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state
            )
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring=cv_scoring)
        
        # Feature importance
        feature_importance = {
            feature: float(importance)
            for feature, importance in zip(features, model.feature_importances_)
        }
        
        # Predictions on full dataset
        y_pred_full = model.predict(X)
        
        result = {
            "model_type": "random_forest",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "coefficients": None,  # Tree models don't have coefficients
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": feature_importance,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
        
        return result
    except Exception as e:
        raise ValueError(f"Error training random forest: {str(e)}")


def train_decision_tree(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    max_depth: Optional[int] = None,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a decision tree model (regression or classification)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Determine task type
        task_type = _determine_task_type(y)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = DecisionTreeRegressor(
                max_depth=max_depth,
                random_state=random_state
            )
        else:
            model = DecisionTreeClassifier(
                max_depth=max_depth,
                random_state=random_state
            )
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring=cv_scoring)
        
        # Feature importance
        feature_importance = {
            feature: float(importance)
            for feature, importance in zip(features, model.feature_importances_)
        }
        
        # Predictions on full dataset
        y_pred_full = model.predict(X)
        
        return {
            "model_type": "decision_tree",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "max_depth": max_depth,
            "coefficients": None,  # Tree models don't have coefficients
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": feature_importance,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training decision tree: {str(e)}")


def train_gradient_boosting(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    n_estimators: int = 100,
    learning_rate: float = 0.1,
    max_depth: Optional[int] = 3,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a gradient boosting model (regression or classification)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Determine task type
        task_type = _determine_task_type(y)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = GradientBoostingRegressor(
                n_estimators=n_estimators,
                learning_rate=learning_rate,
                max_depth=max_depth,
                random_state=random_state
            )
        else:
            model = GradientBoostingClassifier(
                n_estimators=n_estimators,
                learning_rate=learning_rate,
                max_depth=max_depth,
                random_state=random_state
            )
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring=cv_scoring)
        
        # Feature importance
        feature_importance = {
            feature: float(importance)
            for feature, importance in zip(features, model.feature_importances_)
        }
        
        # Predictions on full dataset
        y_pred_full = model.predict(X)
        
        return {
            "model_type": "gradient_boosting",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "n_estimators": n_estimators,
            "learning_rate": learning_rate,
            "max_depth": max_depth,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": feature_importance,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training gradient boosting: {str(e)}")


def train_elasticnet(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train an ElasticNet regression model (L1 + L2 regularization)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=random_state, max_iter=1000)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        # Coefficients
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        # Predictions on full dataset
        y_pred_full = model.predict(X)
        
        return {
            "model_type": "elasticnet",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "alpha": alpha,
            "l1_ratio": l1_ratio,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training ElasticNet: {str(e)}")


def train_svm(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    kernel: str = 'rbf',
    C: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Support Vector Machine model (regression or classification)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Determine task type
        task_type = _determine_task_type(y)
        
        # Scale features for SVM (important for performance)
        scaler = StandardScaler()
        X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=X.columns, index=X.index)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = SVR(kernel=kernel, C=C)
        else:
            model = SVC(kernel=kernel, C=C, random_state=random_state)
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_scaled, y, cv=5, scoring=cv_scoring)
        
        # Predictions on full dataset
        y_pred_full = model.predict(X_scaled)
        
        return {
            "model_type": "svm",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "kernel": kernel,
            "C": C,
            "coefficients": None,  # SVM doesn't have simple coefficients
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": None,  # SVM doesn't have feature importance
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training SVM: {str(e)}")


def train_knn(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    n_neighbors: int = 5,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a K-Nearest Neighbors model (regression or classification)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Determine task type
        task_type = _determine_task_type(y)
        
        # Scale features for KNN (important for distance-based algorithms)
        scaler = StandardScaler()
        X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=X.columns, index=X.index)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = KNeighborsRegressor(n_neighbors=n_neighbors)
        else:
            model = KNeighborsClassifier(n_neighbors=n_neighbors)
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_scaled, y, cv=5, scoring=cv_scoring)
        
        # Predictions on full dataset
        y_pred_full = model.predict(X_scaled)
        
        return {
            "model_type": "knn",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "n_neighbors": n_neighbors,
            "coefficients": None,  # KNN doesn't have coefficients
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": y_pred_full.tolist(),
            "feature_importance": None,  # KNN doesn't have feature importance
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training KNN: {str(e)}")


# ============================================================================
# ADDITIONAL REGRESSION MODELS
# ============================================================================

def train_polynomial_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    degree: int = 2,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a polynomial regression model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Create polynomial features
        poly = PolynomialFeatures(degree=degree, include_bias=False)
        X_poly = poly.fit_transform(X)
        X_poly = pd.DataFrame(X_poly, columns=[f"poly_{i}" for i in range(X_poly.shape[1])])
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_poly, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_poly, y, cv=5, scoring='r2')
        
        return {
            "model_type": "polynomial_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "degree": degree,
            "coefficients": {
                "intercept": float(model.intercept_),
                "n_coefficients": len(model.coef_)
            },
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X_poly).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training polynomial regression: {str(e)}")


def train_bayesian_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    alpha_1: float = 1e-6,
    alpha_2: float = 1e-6,
    lambda_1: float = 1e-6,
    lambda_2: float = 1e-6,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Bayesian ridge regression model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = BayesianRidge(
            alpha_1=alpha_1, alpha_2=alpha_2,
            lambda_1=lambda_1, lambda_2=lambda_2
        )
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        # Coefficients
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        return {
            "model_type": "bayesian_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training Bayesian regression: {str(e)}")


def train_quantile_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    quantile: float = 0.5,
    alpha: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a quantile regression model."""
    try:
        from sklearn.linear_model import QuantileRegressor
        
        X, y = _prepare_data(data, target_variable, features)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = QuantileRegressor(quantile=quantile, alpha=alpha, solver='highs')
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        return {
            "model_type": "quantile_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "quantile": quantile,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except ImportError:
        raise ValueError("QuantileRegressor requires scikit-learn >= 1.0. Please upgrade scikit-learn.")
    except Exception as e:
        raise ValueError(f"Error training quantile regression: {str(e)}")


def train_poisson_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    alpha: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Poisson regression model (for count data)."""
    try:
        from sklearn.linear_model import PoissonRegressor
        
        X, y = _prepare_data(data, target_variable, features)
        
        # Ensure target is non-negative for Poisson
        if (y < 0).any():
            raise ValueError("Poisson regression requires non-negative target values")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = PoissonRegressor(alpha=alpha, max_iter=1000)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        return {
            "model_type": "poisson_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "alpha": alpha,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except ImportError:
        raise ValueError("PoissonRegressor requires scikit-learn >= 0.23. Please upgrade scikit-learn.")
    except Exception as e:
        raise ValueError(f"Error training Poisson regression: {str(e)}")


def train_gamma_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    alpha: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Gamma regression model (for positive continuous data)."""
    try:
        from sklearn.linear_model import GammaRegressor
        
        X, y = _prepare_data(data, target_variable, features)
        
        # Ensure target is positive for Gamma
        if (y <= 0).any():
            raise ValueError("Gamma regression requires positive target values")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = GammaRegressor(alpha=alpha, max_iter=1000)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        return {
            "model_type": "gamma_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "alpha": alpha,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except ImportError:
        raise ValueError("GammaRegressor requires scikit-learn >= 0.23. Please upgrade scikit-learn.")
    except Exception as e:
        raise ValueError(f"Error training Gamma regression: {str(e)}")


def train_tweedie_regression(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    power: float = 0.0,
    alpha: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Tweedie regression model."""
    try:
        from sklearn.linear_model import TweedieRegressor
        
        X, y = _prepare_data(data, target_variable, features)
        
        # Ensure target is non-negative for Tweedie
        if (y < 0).any():
            raise ValueError("Tweedie regression requires non-negative target values")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        model = TweedieRegressor(power=power, alpha=alpha, max_iter=1000)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        coefficients = {
            "intercept": float(model.intercept_),
            "features": {
                feature: float(coef) for feature, coef in zip(features, model.coef_)
            }
        }
        
        return {
            "model_type": "tweedie_regression",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "power": power,
            "alpha": alpha,
            "coefficients": coefficients,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()),
                    "std_r2": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except ImportError:
        raise ValueError("TweedieRegressor requires scikit-learn >= 0.23. Please upgrade scikit-learn.")
    except Exception as e:
        raise ValueError(f"Error training Tweedie regression: {str(e)}")


def train_extra_trees(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    n_estimators: int = 100,
    max_depth: Optional[int] = None,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train an Extra Trees model (regression or classification)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        task_type = _determine_task_type(y)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = ExtraTreesRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state
            )
        else:
            model = ExtraTreesClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state
            )
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring=cv_scoring)
        
        # Feature importance
        feature_importance = {
            feature: float(importance)
            for feature, importance in zip(features, model.feature_importances_)
        }
        
        return {
            "model_type": "extra_trees",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": feature_importance,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training Extra Trees: {str(e)}")


def train_xgboost(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    n_estimators: int = 100,
    max_depth: int = 3,
    learning_rate: float = 0.1,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train an XGBoost model (regression or classification)."""
    if not XGBOOST_AVAILABLE:
        raise ValueError("XGBoost is not installed. Install it with: pip install xgboost")
    
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        task_type = _determine_task_type(y)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = xgb.XGBRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                random_state=random_state
            )
        else:
            model = xgb.XGBClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                random_state=random_state
            )
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring=cv_scoring)
        
        # Feature importance
        feature_importance = {
            feature: float(importance)
            for feature, importance in zip(features, model.feature_importances_)
        }
        
        return {
            "model_type": "xgboost",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": feature_importance,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training XGBoost: {str(e)}")


def train_lightgbm(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    n_estimators: int = 100,
    max_depth: int = -1,
    learning_rate: float = 0.1,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a LightGBM model (regression or classification)."""
    if not LIGHTGBM_AVAILABLE:
        raise ValueError("LightGBM is not installed. Install it with: pip install lightgbm")
    
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        task_type = _determine_task_type(y)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = lgb.LGBMRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                random_state=random_state,
                verbose=-1
            )
        else:
            model = lgb.LGBMClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                random_state=random_state,
                verbose=-1
            )
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring=cv_scoring)
        
        # Feature importance
        feature_importance = {
            feature: float(importance)
            for feature, importance in zip(features, model.feature_importances_)
        }
        
        return {
            "model_type": "lightgbm",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": feature_importance,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training LightGBM: {str(e)}")


def train_catboost(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    iterations: int = 100,
    depth: int = 6,
    learning_rate: float = 0.1,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a CatBoost model (regression or classification)."""
    if not CATBOOST_AVAILABLE:
        raise ValueError("CatBoost is not installed. Install it with: pip install catboost")
    
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        task_type = _determine_task_type(y)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = cb.CatBoostRegressor(
                iterations=iterations,
                depth=depth,
                learning_rate=learning_rate,
                random_state=random_state,
                verbose=False
            )
        else:
            model = cb.CatBoostClassifier(
                iterations=iterations,
                depth=depth,
                learning_rate=learning_rate,
                random_state=random_state,
                verbose=False
            )
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring=cv_scoring)
        
        # Feature importance
        feature_importance = {
            feature: float(importance)
            for feature, importance in zip(features, model.feature_importances_)
        }
        
        return {
            "model_type": "catboost",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "iterations": iterations,
            "depth": depth,
            "learning_rate": learning_rate,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": feature_importance,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training CatBoost: {str(e)}")


def train_gaussian_process(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Gaussian Process regression model."""
    if not GAUSSIAN_PROCESS_AVAILABLE:
        raise ValueError("Gaussian Process requires scikit-learn >= 0.18")
    
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Limit data size for GP (can be slow)
        if len(X) > 1000:
            X = X.sample(n=1000, random_state=random_state)
            y = y.loc[X.index]
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Train model
        kernel = C(1.0, (1e-3, 1e3)) * RBF(1.0, (1e-2, 1e2))
        model = GaussianProcessRegressor(kernel=kernel, random_state=random_state, n_restarts_optimizer=5)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
        
        # Cross-validation (on smaller subset)
        cv_scores = cross_val_score(model, X_train, y_train, cv=min(5, len(X_train)//10), scoring='r2')
        
        return {
            "model_type": "gaussian_process",
            "task_type": "regression",
            "target_variable": target_variable,
            "features": features,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_r2": float(cv_scores.mean()) if len(cv_scores) > 0 else 0.0,
                    "std_r2": float(cv_scores.std()) if len(cv_scores) > 0 else 0.0
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training Gaussian Process: {str(e)}")


def train_mlp(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    hidden_layer_sizes: tuple = (100,),
    activation: str = 'relu',
    solver: str = 'adam',
    alpha: float = 0.0001,
    learning_rate: str = 'constant',
    max_iter: int = 200,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Multi-Layer Perceptron (MLP) model (regression or classification)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Scale features for MLP
        scaler = StandardScaler()
        X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=X.columns, index=X.index)
        
        task_type = _determine_task_type(y)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=test_size, random_state=random_state,
            stratify=y if task_type == "classification" and y.nunique() > 1 else None
        )
        
        # Train model
        if task_type == "regression":
            model = MLPRegressor(
                hidden_layer_sizes=hidden_layer_sizes,
                activation=activation,
                solver=solver,
                alpha=alpha,
                learning_rate=learning_rate,
                max_iter=max_iter,
                random_state=random_state
            )
        else:
            model = MLPClassifier(
                hidden_layer_sizes=hidden_layer_sizes,
                activation=activation,
                solver=solver,
                alpha=alpha,
                learning_rate=learning_rate,
                max_iter=max_iter,
                random_state=random_state
            )
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        if task_type == "regression":
            train_metrics = _calculate_regression_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_regression_metrics(y_test.values, y_test_pred)
            cv_scoring = 'r2'
        else:
            train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
            test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
            cv_scoring = 'accuracy'
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_scaled, y, cv=5, scoring=cv_scoring)
        
        return {
            "model_type": "mlp",
            "task_type": task_type,
            "target_variable": target_variable,
            "features": features,
            "hidden_layer_sizes": hidden_layer_sizes,
            "activation": activation,
            "solver": solver,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    f"mean_{cv_scoring}": float(cv_scores.mean()),
                    f"std_{cv_scoring}": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X_scaled).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training MLP: {str(e)}")


# ============================================================================
# ADDITIONAL CLASSIFICATION MODELS
# ============================================================================

def train_multinomial_logistic(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a multinomial logistic regression model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Ensure we have multiple classes
        unique_classes = y.nunique()
        if unique_classes < 2:
            raise ValueError("Multinomial logistic regression requires at least 2 classes")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if unique_classes > 1 else None
        )
        
        # Train model
        model = LogisticRegression(multi_class='multinomial', max_iter=1000, random_state=random_state)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='accuracy')
        
        return {
            "model_type": "multinomial_logistic",
            "task_type": "classification",
            "target_variable": target_variable,
            "features": features,
            "n_classes": unique_classes,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_accuracy": float(cv_scores.mean()),
                    "std_accuracy": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training multinomial logistic regression: {str(e)}")


def train_naive_bayes(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    variant: str = 'gaussian',
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Naive Bayes model (Gaussian, Multinomial, or Bernoulli)."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if y.nunique() > 1 else None
        )
        
        # Train model based on variant
        if variant.lower() == 'gaussian':
            model = GaussianNB()
        elif variant.lower() == 'multinomial':
            # Ensure non-negative values for multinomial
            if (X < 0).any().any():
                raise ValueError("Multinomial Naive Bayes requires non-negative feature values")
            model = MultinomialNB()
        elif variant.lower() == 'bernoulli':
            # Binarize features for Bernoulli
            X_train = (X_train > X_train.mean()).astype(int)
            X_test = (X_test > X_test.mean()).astype(int)
            model = BernoulliNB()
        else:
            raise ValueError(f"Unknown Naive Bayes variant: {variant}. Use 'gaussian', 'multinomial', or 'bernoulli'")
        
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='accuracy')
        
        return {
            "model_type": f"naive_bayes_{variant.lower()}",
            "task_type": "classification",
            "target_variable": target_variable,
            "features": features,
            "variant": variant.lower(),
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_accuracy": float(cv_scores.mean()),
                    "std_accuracy": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X_train).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training Naive Bayes ({variant}): {str(e)}")


def train_lda(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Linear Discriminant Analysis (LDA) model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Ensure we have multiple classes
        unique_classes = y.nunique()
        if unique_classes < 2:
            raise ValueError("LDA requires at least 2 classes")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if unique_classes > 1 else None
        )
        
        # Train model
        model = LinearDiscriminantAnalysis()
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='accuracy')
        
        return {
            "model_type": "lda",
            "task_type": "classification",
            "target_variable": target_variable,
            "features": features,
            "n_classes": unique_classes,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_accuracy": float(cv_scores.mean()),
                    "std_accuracy": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training LDA: {str(e)}")


def train_qda(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a Quadratic Discriminant Analysis (QDA) model."""
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Ensure we have multiple classes
        unique_classes = y.nunique()
        if unique_classes < 2:
            raise ValueError("QDA requires at least 2 classes")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state,
            stratify=y if unique_classes > 1 else None
        )
        
        # Train model
        model = QuadraticDiscriminantAnalysis()
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_metrics = _calculate_classification_metrics(y_train.values, y_train_pred)
        test_metrics = _calculate_classification_metrics(y_test.values, y_test_pred)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='accuracy')
        
        return {
            "model_type": "qda",
            "task_type": "classification",
            "target_variable": target_variable,
            "features": features,
            "n_classes": unique_classes,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {
                    "mean_accuracy": float(cv_scores.mean()),
                    "std_accuracy": float(cv_scores.std())
                }
            },
            "predictions": model.predict(X).tolist(),
            "feature_importance": None,
            "n_samples": len(X),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training QDA: {str(e)}")


# ============================================================================
# UNSUPERVISED LEARNING MODELS - CLUSTERING
# ============================================================================

def train_kmeans(
    data: List[Dict[str, Any]],
    features: List[str],
    n_clusters: int = 3,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a K-Means clustering model."""
    try:
        X, _ = _prepare_data(data, features[0], features)  # Use first feature as dummy target
        X = X[features]  # Get only the features
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
        labels = model.fit_predict(X_scaled)
        
        # Calculate silhouette score
        try:
            silhouette = float(silhouette_score(X_scaled, labels))
        except:
            silhouette = 0.0
        
        return {
            "model_type": "kmeans",
            "task_type": "clustering",
            "features": features,
            "n_clusters": n_clusters,
            "labels": labels.tolist(),
            "inertia": float(model.inertia_),
            "silhouette_score": silhouette,
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training K-Means: {str(e)}")


def train_dbscan(
    data: List[Dict[str, Any]],
    features: List[str],
    eps: float = 0.5,
    min_samples: int = 5
) -> Dict[str, Any]:
    """Train a DBSCAN clustering model."""
    try:
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = DBSCAN(eps=eps, min_samples=min_samples)
        labels = model.fit_predict(X_scaled)
        
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = list(labels).count(-1)
        
        # Calculate silhouette score (only if we have clusters)
        try:
            if n_clusters > 1:
                silhouette = float(silhouette_score(X_scaled, labels))
            else:
                silhouette = -1.0
        except:
            silhouette = -1.0
        
        return {
            "model_type": "dbscan",
            "task_type": "clustering",
            "features": features,
            "n_clusters": n_clusters,
            "n_noise": n_noise,
            "labels": labels.tolist(),
            "silhouette_score": silhouette,
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training DBSCAN: {str(e)}")


def train_hierarchical_clustering(
    data: List[Dict[str, Any]],
    features: List[str],
    n_clusters: int = 3,
    linkage: str = 'ward'
) -> Dict[str, Any]:
    """Train a Hierarchical/Agglomerative clustering model."""
    try:
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = AgglomerativeClustering(n_clusters=n_clusters, linkage=linkage)
        labels = model.fit_predict(X_scaled)
        
        # Calculate silhouette score
        try:
            silhouette = float(silhouette_score(X_scaled, labels))
        except:
            silhouette = 0.0
        
        return {
            "model_type": "hierarchical_clustering",
            "task_type": "clustering",
            "features": features,
            "n_clusters": n_clusters,
            "linkage": linkage,
            "labels": labels.tolist(),
            "silhouette_score": silhouette,
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training Hierarchical Clustering: {str(e)}")


# ============================================================================
# DIMENSIONALITY REDUCTION
# ============================================================================

def train_pca(
    data: List[Dict[str, Any]],
    features: List[str],
    n_components: Optional[int] = None
) -> Dict[str, Any]:
    """Train a Principal Component Analysis (PCA) model."""
    try:
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = PCA(n_components=n_components)
        X_transformed = model.fit_transform(X_scaled)
        
        # Calculate explained variance
        explained_variance_ratio = model.explained_variance_ratio_.tolist()
        cumulative_variance = np.cumsum(explained_variance_ratio).tolist()
        
        return {
            "model_type": "pca",
            "task_type": "dimensionality_reduction",
            "features": features,
            "n_components": model.n_components_,
            "explained_variance_ratio": explained_variance_ratio,
            "cumulative_variance": cumulative_variance,
            "transformed_data": X_transformed.tolist(),
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training PCA: {str(e)}")


def train_tsne(
    data: List[Dict[str, Any]],
    features: List[str],
    n_components: int = 2,
    perplexity: float = 30.0,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a t-SNE dimensionality reduction model."""
    try:
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Limit data size for t-SNE (can be slow)
        if len(X) > 1000:
            X = X.sample(n=1000, random_state=random_state)
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = TSNE(n_components=n_components, perplexity=perplexity, random_state=random_state)
        X_transformed = model.fit_transform(X_scaled)
        
        return {
            "model_type": "tsne",
            "task_type": "dimensionality_reduction",
            "features": features,
            "n_components": n_components,
            "perplexity": perplexity,
            "transformed_data": X_transformed.tolist(),
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training t-SNE: {str(e)}")


def train_umap(
    data: List[Dict[str, Any]],
    features: List[str],
    n_components: int = 2,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a UMAP dimensionality reduction model."""
    if not UMAP_AVAILABLE:
        raise ValueError("UMAP is not installed. Install it with: pip install umap-learn")
    
    try:
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = UMAP(n_components=n_components, n_neighbors=n_neighbors, min_dist=min_dist, random_state=random_state)
        X_transformed = model.fit_transform(X_scaled)
        
        return {
            "model_type": "umap",
            "task_type": "dimensionality_reduction",
            "features": features,
            "n_components": n_components,
            "transformed_data": X_transformed.tolist(),
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training UMAP: {str(e)}")


# ============================================================================
# TIME SERIES MODELS
# ============================================================================

def train_arima(
    data: List[Dict[str, Any]],
    target_variable: str,
    date_column: Optional[str] = None,
    order: tuple = (1, 1, 1),
    seasonal_order: Optional[tuple] = None
) -> Dict[str, Any]:
    """Train an ARIMA or SARIMA time series model."""
    if not STATSMODELS_AVAILABLE:
        raise ValueError("statsmodels is not installed. Install it with: pip install statsmodels")
    
    try:
        df = pd.DataFrame(data)
        
        if target_variable not in df.columns:
            raise ValueError(f"Target variable '{target_variable}' not found in data")
        
        # Extract time series
        if date_column and date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            df = df.sort_values(date_column)
            ts = df[target_variable].dropna()
        else:
            ts = pd.Series(df[target_variable].dropna())
        
        if len(ts) < 10:
            raise ValueError("Time series must have at least 10 observations")
        
        # Train model
        if seasonal_order:
            model = SARIMAX(ts, order=order, seasonal_order=seasonal_order)
        else:
            model = ARIMA(ts, order=order)
        
        fitted_model = model.fit()
        
        # Forecast
        forecast = fitted_model.forecast(steps=min(10, len(ts) // 4))
        
        # Get model summary metrics
        aic = float(fitted_model.aic) if hasattr(fitted_model, 'aic') else None
        bic = float(fitted_model.bic) if hasattr(fitted_model, 'bic') else None
        
        return {
            "model_type": "sarima" if seasonal_order else "arima",
            "task_type": "time_series",
            "target_variable": target_variable,
            "order": order,
            "seasonal_order": seasonal_order,
            "aic": aic,
            "bic": bic,
            "forecast": forecast.tolist(),
            "n_samples": len(ts),
            "fitted_values": fitted_model.fittedvalues.tolist()
        }
    except Exception as e:
        raise ValueError(f"Error training ARIMA/SARIMA: {str(e)}")


def train_exponential_smoothing(
    data: List[Dict[str, Any]],
    target_variable: str,
    date_column: Optional[str] = None,
    trend: Optional[str] = None,
    seasonal: Optional[str] = None,
    seasonal_periods: Optional[int] = None
) -> Dict[str, Any]:
    """Train an Exponential Smoothing time series model."""
    if not STATSMODELS_AVAILABLE:
        raise ValueError("statsmodels is not installed. Install it with: pip install statsmodels")
    
    try:
        df = pd.DataFrame(data)
        
        if target_variable not in df.columns:
            raise ValueError(f"Target variable '{target_variable}' not found in data")
        
        # Extract time series
        if date_column and date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            df = df.sort_values(date_column)
            ts = df[target_variable].dropna()
        else:
            ts = pd.Series(df[target_variable].dropna())
        
        if len(ts) < 10:
            raise ValueError("Time series must have at least 10 observations")
        
        # Train model
        model = ExponentialSmoothing(
            ts,
            trend=trend,
            seasonal=seasonal,
            seasonal_periods=seasonal_periods
        )
        fitted_model = model.fit()
        
        # Forecast
        forecast = fitted_model.forecast(steps=min(10, len(ts) // 4))
        
        return {
            "model_type": "exponential_smoothing",
            "task_type": "time_series",
            "target_variable": target_variable,
            "trend": trend,
            "seasonal": seasonal,
            "seasonal_periods": seasonal_periods,
            "aic": float(fitted_model.aic) if hasattr(fitted_model, 'aic') else None,
            "forecast": forecast.tolist(),
            "n_samples": len(ts),
            "fitted_values": fitted_model.fittedvalues.tolist()
        }
    except Exception as e:
        raise ValueError(f"Error training Exponential Smoothing: {str(e)}")


def train_lstm(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    sequence_length: int = 10,
    lstm_units: int = 50,
    epochs: int = 50,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train an LSTM time series model."""
    try:
        import tensorflow as tf
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.layers import LSTM, Dense, Dropout
        from tensorflow.keras.optimizers import Adam
    except ImportError:
        raise ValueError("TensorFlow is not installed. Install it with: pip install tensorflow")
    
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Create sequences for LSTM
        def create_sequences(data, seq_length):
            X_seq, y_seq = [], []
            for i in range(len(data) - seq_length):
                X_seq.append(data[i:i+seq_length])
                y_seq.append(data[i+seq_length])
            return np.array(X_seq), np.array(y_seq)
        
        # Prepare data
        data_array = X.values
        X_seq, y_seq = create_sequences(data_array, sequence_length)
        
        if len(X_seq) < 10:
            raise ValueError(f"Need at least {sequence_length + 10} samples for LSTM")
        
        # Split data
        split_idx = int(len(X_seq) * (1 - test_size))
        X_train, X_test = X_seq[:split_idx], X_seq[split_idx:]
        y_train, y_test = y_seq[:split_idx], y_seq[split_idx:]
        
        # Reshape for LSTM (samples, timesteps, features)
        X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], X_train.shape[2]))
        X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], X_test.shape[2]))
        
        # Build model
        model = Sequential([
            LSTM(lstm_units, activation='relu', input_shape=(sequence_length, len(features))),
            Dropout(0.2),
            Dense(1)
        ])
        model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae'])
        
        # Train model
        history = model.fit(
            X_train, y_train,
            epochs=epochs,
            batch_size=32,
            validation_data=(X_test, y_test),
            verbose=0
        )
        
        # Predictions
        y_train_pred = model.predict(X_train, verbose=0).flatten()
        y_test_pred = model.predict(X_test, verbose=0).flatten()
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test, y_test_pred)
        
        return {
            "model_type": "lstm",
            "task_type": "time_series",
            "target_variable": target_variable,
            "features": features,
            "sequence_length": sequence_length,
            "lstm_units": lstm_units,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {}
            },
            "predictions": model.predict(X_seq, verbose=0).flatten().tolist(),
            "feature_importance": None,
            "n_samples": len(X_seq),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training LSTM: {str(e)}")


def train_gru(
    data: List[Dict[str, Any]],
    target_variable: str,
    features: List[str],
    sequence_length: int = 10,
    gru_units: int = 50,
    epochs: int = 50,
    test_size: float = 0.2,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a GRU time series model."""
    try:
        import tensorflow as tf
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.layers import GRU, Dense, Dropout
        from tensorflow.keras.optimizers import Adam
    except ImportError:
        raise ValueError("TensorFlow is not installed. Install it with: pip install tensorflow")
    
    try:
        X, y = _prepare_data(data, target_variable, features)
        
        # Create sequences for GRU
        def create_sequences(data, seq_length):
            X_seq, y_seq = [], []
            for i in range(len(data) - seq_length):
                X_seq.append(data[i:i+seq_length])
                y_seq.append(data[i+seq_length])
            return np.array(X_seq), np.array(y_seq)
        
        # Prepare data
        data_array = X.values
        X_seq, y_seq = create_sequences(data_array, sequence_length)
        
        if len(X_seq) < 10:
            raise ValueError(f"Need at least {sequence_length + 10} samples for GRU")
        
        # Split data
        split_idx = int(len(X_seq) * (1 - test_size))
        X_train, X_test = X_seq[:split_idx], X_seq[split_idx:]
        y_train, y_test = y_seq[:split_idx], y_seq[split_idx:]
        
        # Reshape for GRU
        X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], X_train.shape[2]))
        X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], X_test.shape[2]))
        
        # Build model
        model = Sequential([
            GRU(gru_units, activation='relu', input_shape=(sequence_length, len(features))),
            Dropout(0.2),
            Dense(1)
        ])
        model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae'])
        
        # Train model
        history = model.fit(
            X_train, y_train,
            epochs=epochs,
            batch_size=32,
            validation_data=(X_test, y_test),
            verbose=0
        )
        
        # Predictions
        y_train_pred = model.predict(X_train, verbose=0).flatten()
        y_test_pred = model.predict(X_test, verbose=0).flatten()
        
        # Metrics
        train_metrics = _calculate_regression_metrics(y_train, y_train_pred)
        test_metrics = _calculate_regression_metrics(y_test, y_test_pred)
        
        return {
            "model_type": "gru",
            "task_type": "time_series",
            "target_variable": target_variable,
            "features": features,
            "sequence_length": sequence_length,
            "gru_units": gru_units,
            "coefficients": None,
            "metrics": {
                "train": train_metrics,
                "test": test_metrics,
                "cross_validation": {}
            },
            "predictions": model.predict(X_seq, verbose=0).flatten().tolist(),
            "feature_importance": None,
            "n_samples": len(X_seq),
            "n_train": len(X_train),
            "n_test": len(X_test)
        }
    except Exception as e:
        raise ValueError(f"Error training GRU: {str(e)}")


# ============================================================================
# ANOMALY DETECTION MODELS
# ============================================================================

def train_isolation_forest(
    data: List[Dict[str, Any]],
    features: List[str],
    contamination: float = 0.1,
    n_estimators: int = 100,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train an Isolation Forest anomaly detection model."""
    try:
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=random_state
        )
        predictions = model.fit_predict(X_scaled)
        
        # -1 for anomalies, 1 for normal
        anomaly_indices = np.where(predictions == -1)[0].tolist()
        n_anomalies = len(anomaly_indices)
        
        return {
            "model_type": "isolation_forest",
            "task_type": "anomaly_detection",
            "features": features,
            "contamination": contamination,
            "n_anomalies": n_anomalies,
            "anomaly_indices": anomaly_indices,
            "anomaly_scores": model.score_samples(X_scaled).tolist(),
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training Isolation Forest: {str(e)}")


def train_one_class_svm(
    data: List[Dict[str, Any]],
    features: List[str],
    nu: float = 0.1,
    kernel: str = 'rbf',
    random_state: int = 42
) -> Dict[str, Any]:
    """Train a One-Class SVM anomaly detection model."""
    try:
        from sklearn.svm import OneClassSVM
        
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = OneClassSVM(nu=nu, kernel=kernel)
        predictions = model.fit_predict(X_scaled)
        
        # -1 for anomalies, 1 for normal
        anomaly_indices = np.where(predictions == -1)[0].tolist()
        n_anomalies = len(anomaly_indices)
        
        return {
            "model_type": "one_class_svm",
            "task_type": "anomaly_detection",
            "features": features,
            "nu": nu,
            "kernel": kernel,
            "n_anomalies": n_anomalies,
            "anomaly_indices": anomaly_indices,
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training One-Class SVM: {str(e)}")


def train_local_outlier_factor(
    data: List[Dict[str, Any]],
    features: List[str],
    n_neighbors: int = 20,
    contamination: float = 0.1
) -> Dict[str, Any]:
    """Train a Local Outlier Factor (LOF) anomaly detection model."""
    try:
        from sklearn.neighbors import LocalOutlierFactor
        
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination)
        predictions = model.fit_predict(X_scaled)
        
        # -1 for anomalies, 1 for normal
        anomaly_indices = np.where(predictions == -1)[0].tolist()
        n_anomalies = len(anomaly_indices)
        
        return {
            "model_type": "local_outlier_factor",
            "task_type": "anomaly_detection",
            "features": features,
            "n_neighbors": n_neighbors,
            "contamination": contamination,
            "n_anomalies": n_anomalies,
            "anomaly_indices": anomaly_indices,
            "outlier_scores": model.negative_outlier_factor_.tolist(),
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training Local Outlier Factor: {str(e)}")


def train_elliptic_envelope(
    data: List[Dict[str, Any]],
    features: List[str],
    contamination: float = 0.1,
    random_state: int = 42
) -> Dict[str, Any]:
    """Train an Elliptic Envelope anomaly detection model."""
    try:
        X, _ = _prepare_data(data, features[0], features)
        X = X[features]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train model
        model = EllipticEnvelope(contamination=contamination, random_state=random_state)
        predictions = model.fit_predict(X_scaled)
        
        # -1 for anomalies, 1 for normal
        anomaly_indices = np.where(predictions == -1)[0].tolist()
        n_anomalies = len(anomaly_indices)
        
        return {
            "model_type": "elliptic_envelope",
            "task_type": "anomaly_detection",
            "features": features,
            "contamination": contamination,
            "n_anomalies": n_anomalies,
            "anomaly_indices": anomaly_indices,
            "n_samples": len(X)
        }
    except Exception as e:
        raise ValueError(f"Error training Elliptic Envelope: {str(e)}")


# ============================================================================
# RECOMMENDATION SYSTEMS
# ============================================================================

def train_matrix_factorization(
    data: List[Dict[str, Any]],
    user_column: str,
    item_column: str,
    rating_column: str,
    n_factors: int = 50,
    n_epochs: int = 20,
    learning_rate: float = 0.01,
    regularization: float = 0.1
) -> Dict[str, Any]:
    """Train a Matrix Factorization recommendation model (simplified ALS)."""
    try:
        df = pd.DataFrame(data)
        
        # Validate columns
        required_cols = [user_column, item_column, rating_column]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        # Create user-item matrix
        user_item_matrix = df.pivot_table(
            index=user_column,
            columns=item_column,
            values=rating_column,
            fill_value=0
        )
        
        # Simple matrix factorization using SVD
        from sklearn.decomposition import NMF
        
        # Use NMF for non-negative matrix factorization
        model = NMF(n_components=n_factors, random_state=42, max_iter=n_epochs)
        W = model.fit_transform(user_item_matrix)
        H = model.components_
        
        # Reconstruct matrix
        reconstructed = np.dot(W, H)
        
        # Calculate reconstruction error
        mse = np.mean((user_item_matrix.values - reconstructed) ** 2)
        
        return {
            "model_type": "matrix_factorization",
            "task_type": "recommendation",
            "user_column": user_column,
            "item_column": item_column,
            "rating_column": rating_column,
            "n_factors": n_factors,
            "n_users": len(user_item_matrix),
            "n_items": len(user_item_matrix.columns),
            "reconstruction_error": float(mse),
            "n_samples": len(df)
        }
    except Exception as e:
        raise ValueError(f"Error training Matrix Factorization: {str(e)}")


# ============================================================================
# SURVIVAL ANALYSIS
# ============================================================================

def train_cox_proportional_hazards(
    data: List[Dict[str, Any]],
    duration_column: str,
    event_column: str,
    features: List[str]
) -> Dict[str, Any]:
    """Train a Cox Proportional Hazards survival analysis model."""
    try:
        from lifelines import CoxPHFitter
    except ImportError:
        raise ValueError("lifelines is not installed. Install it with: pip install lifelines")
    
    try:
        df = pd.DataFrame(data)
        
        # Validate columns
        required_cols = [duration_column, event_column] + features
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        # Prepare data
        survival_data = df[[duration_column, event_column] + features].copy()
        survival_data = survival_data.dropna()
        
        if len(survival_data) < 10:
            raise ValueError("Need at least 10 samples for survival analysis")
        
        # Train model
        cph = CoxPHFitter()
        cph.fit(survival_data, duration_column=duration_column, event_col=event_column)
        
        # Get summary
        summary = cph.summary
        
        return {
            "model_type": "cox_proportional_hazards",
            "task_type": "survival_analysis",
            "duration_column": duration_column,
            "event_column": event_column,
            "features": features,
            "concordance_index": float(cph.concordance_index_) if hasattr(cph, 'concordance_index_') else None,
            "coefficients": {
                feature: float(cph.hazard_ratios_[feature]) if feature in cph.hazard_ratios_.index else None
                for feature in features
            },
            "n_samples": len(survival_data)
        }
    except Exception as e:
        raise ValueError(f"Error training Cox Proportional Hazards: {str(e)}")


def train_kaplan_meier(
    data: List[Dict[str, Any]],
    duration_column: str,
    event_column: str,
    group_column: Optional[str] = None
) -> Dict[str, Any]:
    """Train a Kaplan-Meier survival estimator."""
    try:
        from lifelines import KaplanMeierFitter
    except ImportError:
        raise ValueError("lifelines is not installed. Install it with: pip install lifelines")
    
    try:
        df = pd.DataFrame(data)
        
        # Validate columns
        required_cols = [duration_column, event_column]
        if group_column:
            required_cols.append(group_column)
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        # Prepare data
        survival_data = df[[duration_column, event_column]].copy()
        if group_column:
            survival_data[group_column] = df[group_column]
        survival_data = survival_data.dropna()
        
        if len(survival_data) < 10:
            raise ValueError("Need at least 10 samples for survival analysis")
        
        # Train model
        kmf = KaplanMeierFitter()
        
        if group_column:
            # Fit for each group
            groups = survival_data[group_column].unique()
            results = {}
            for group in groups:
                group_data = survival_data[survival_data[group_column] == group]
                kmf.fit(group_data[duration_column], group_data[event_column], label=str(group))
                results[str(group)] = {
                    "median_survival": float(kmf.median_survival_time_) if hasattr(kmf, 'median_survival_time_') else None,
                    "n_samples": len(group_data)
                }
        else:
            kmf.fit(survival_data[duration_column], survival_data[event_column])
            results = {
                "median_survival": float(kmf.median_survival_time_) if hasattr(kmf, 'median_survival_time_') else None,
                "n_samples": len(survival_data)
            }
        
        return {
            "model_type": "kaplan_meier",
            "task_type": "survival_analysis",
            "duration_column": duration_column,
            "event_column": event_column,
            "group_column": group_column,
            "results": results,
            "n_samples": len(survival_data)
        }
    except Exception as e:
        raise ValueError(f"Error training Kaplan-Meier: {str(e)}")
