"""Machine Learning Model Training Functions"""
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Literal, Union
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, GradientBoostingRegressor, GradientBoostingClassifier
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier
from sklearn.svm import SVR, SVC
from sklearn.neighbors import KNeighborsRegressor, KNeighborsClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    r2_score, mean_squared_error, mean_absolute_error,
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report
)
import traceback


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
        if task_type != "classification":
            # Convert to binary classification if needed
            median = y.median()
            y = (y > median).astype(int)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y if task_type == "classification" else None
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
