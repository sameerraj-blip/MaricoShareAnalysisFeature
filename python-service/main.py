"""FastAPI application for Data Operations Service"""
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Literal
import uvicorn
from config import config
from data_operations import remove_nulls, get_preview, get_summary, convert_type, create_derived_column
from ml_models import (
    train_linear_regression,
    train_logistic_regression,
    train_ridge_regression,
    train_lasso_regression,
    train_random_forest,
    train_decision_tree,
    train_gradient_boosting,
    train_elasticnet,
    train_svm,
    train_knn,
    train_polynomial_regression,
    train_bayesian_regression,
    train_quantile_regression,
    train_poisson_regression,
    train_gamma_regression,
    train_tweedie_regression,
    train_extra_trees,
    train_xgboost,
    train_lightgbm,
    train_catboost,
    train_gaussian_process,
    train_mlp,
    train_multinomial_logistic,
    train_naive_bayes,
    train_lda,
    train_qda,
    train_kmeans,
    train_dbscan,
    train_hierarchical_clustering,
    train_pca,
    train_tsne,
    train_umap,
    train_arima,
    train_exponential_smoothing,
    train_lstm,
    train_gru,
    train_isolation_forest,
    train_one_class_svm,
    train_local_outlier_factor,
    train_elliptic_envelope,
    train_matrix_factorization,
    train_cox_proportional_hazards,
    train_kaplan_meier
)
import traceback

app = FastAPI(title="Data Operations Service", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response models
class RemoveNullsRequest(BaseModel):
    data: List[Dict[str, Any]]
    column: Optional[str] = None
    method: Literal["delete", "mean", "median", "mode", "custom"] = "delete"
    custom_value: Optional[Any] = None


class PreviewRequest(BaseModel):
    data: List[Dict[str, Any]]
    limit: int = Field(default=50, ge=1, le=10000)


class CreateDerivedColumnRequest(BaseModel):
    data: List[Dict[str, Any]]
    new_column_name: str
    expression: str


class ConvertTypeRequest(BaseModel):
    data: List[Dict[str, Any]]
    column: str
    target_type: Literal["numeric", "string", "date", "percentage", "boolean"]


class TrainModelRequest(BaseModel):
    data: List[Dict[str, Any]]
    model_type: Literal[
        "linear", "logistic", "ridge", "lasso", "random_forest", "decision_tree", 
        "gradient_boosting", "elasticnet", "svm", "knn",
        "polynomial", "bayesian", "quantile", "poisson", "gamma", "tweedie",
        "extra_trees", "xgboost", "lightgbm", "catboost", "gaussian_process", "mlp",
        "multinomial_logistic", "naive_bayes_gaussian", "naive_bayes_multinomial", "naive_bayes_bernoulli",
        "lda", "qda",
        "kmeans", "dbscan", "hierarchical_clustering",
        "pca", "tsne", "umap",
        "arima", "sarima", "exponential_smoothing", "lstm", "gru",
        "isolation_forest", "one_class_svm", "local_outlier_factor", "elliptic_envelope",
        "matrix_factorization",
        "cox_proportional_hazards", "kaplan_meier"
    ]
    target_variable: Optional[str] = None  # Optional for unsupervised models
    features: List[str]
    test_size: float = Field(default=0.2, ge=0.1, le=0.5)
    random_state: int = Field(default=42)
    # Regression/Classification parameters
    alpha: Optional[float] = Field(default=None, ge=0.0)
    l1_ratio: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    n_estimators: Optional[int] = Field(default=None, ge=1)
    max_depth: Optional[int] = Field(default=None, ge=1)
    learning_rate: Optional[float] = Field(default=None, gt=0.0)
    kernel: Optional[str] = Field(default=None)
    C: Optional[float] = Field(default=None, gt=0.0)
    n_neighbors: Optional[int] = Field(default=None, ge=1)
    # Additional parameters
    degree: Optional[int] = Field(default=None, ge=1)  # Polynomial
    quantile: Optional[float] = Field(default=None, ge=0.0, le=1.0)  # Quantile regression
    power: Optional[float] = Field(default=None)  # Tweedie
    iterations: Optional[int] = Field(default=None, ge=1)  # CatBoost
    depth: Optional[int] = Field(default=None, ge=1)  # CatBoost
    hidden_layer_sizes: Optional[List[int]] = Field(default=None)  # MLP
    activation: Optional[str] = Field(default=None)  # MLP
    solver: Optional[str] = Field(default=None)  # MLP
    max_iter: Optional[int] = Field(default=None, ge=1)  # MLP
    variant: Optional[str] = Field(default=None)  # Naive Bayes
    # Clustering parameters
    n_clusters: Optional[int] = Field(default=None, ge=2)  # K-Means, Hierarchical
    eps: Optional[float] = Field(default=None, gt=0.0)  # DBSCAN
    min_samples: Optional[int] = Field(default=None, ge=1)  # DBSCAN
    linkage: Optional[str] = Field(default=None)  # Hierarchical
    # Dimensionality reduction parameters
    n_components: Optional[int] = Field(default=None, ge=1)  # PCA, t-SNE, UMAP
    perplexity: Optional[float] = Field(default=None, gt=0.0)  # t-SNE
    min_dist: Optional[float] = Field(default=None, ge=0.0)  # UMAP
    # Time series parameters
    date_column: Optional[str] = Field(default=None)  # Time series models
    order: Optional[List[int]] = Field(default=None)  # ARIMA order (p, d, q)
    seasonal_order: Optional[List[int]] = Field(default=None)  # SARIMA seasonal order
    trend: Optional[str] = Field(default=None)  # Exponential smoothing
    seasonal: Optional[str] = Field(default=None)  # Exponential smoothing
    seasonal_periods: Optional[int] = Field(default=None, ge=1)  # Exponential smoothing
    sequence_length: Optional[int] = Field(default=None, ge=1)  # LSTM, GRU
    lstm_units: Optional[int] = Field(default=None, ge=1)  # LSTM
    gru_units: Optional[int] = Field(default=None, ge=1)  # GRU
    epochs: Optional[int] = Field(default=None, ge=1)  # LSTM, GRU
    # Anomaly detection parameters
    contamination: Optional[float] = Field(default=None, ge=0.0, le=0.5)  # Anomaly detection
    nu: Optional[float] = Field(default=None, ge=0.0, le=1.0)  # One-Class SVM
    # Recommendation system parameters
    user_column: Optional[str] = Field(default=None)
    item_column: Optional[str] = Field(default=None)
    rating_column: Optional[str] = Field(default=None)
    n_factors: Optional[int] = Field(default=None, ge=1)  # Matrix factorization
    n_epochs: Optional[int] = Field(default=None, ge=1)  # Matrix factorization
    regularization: Optional[float] = Field(default=None, ge=0.0)  # Matrix factorization
    # Survival analysis parameters
    duration_column: Optional[str] = Field(default=None)
    event_column: Optional[str] = Field(default=None)
    group_column: Optional[str] = Field(default=None)  # Kaplan-Meier


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "service": "data-ops"}


@app.post("/remove-nulls")
async def remove_nulls_endpoint(request: RemoveNullsRequest):
    """Remove null values from data"""
    try:
        if len(request.data) > config.MAX_ROWS:
            raise HTTPException(
                status_code=400,
                detail=f"Data exceeds maximum rows limit of {config.MAX_ROWS}"
            )
        
        result = remove_nulls(
            data=request.data,
            column=request.column,
            method=request.method,
            custom_value=request.custom_value
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error in remove_nulls: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/preview")
async def preview_endpoint(request: PreviewRequest):
    """Get data preview"""
    try:
        if len(request.data) > config.MAX_ROWS:
            raise HTTPException(
                status_code=400,
                detail=f"Data exceeds maximum rows limit of {config.MAX_ROWS}"
            )
        
        if request.limit > config.MAX_PREVIEW_ROWS:
            request.limit = config.MAX_PREVIEW_ROWS
        
        result = get_preview(data=request.data, limit=request.limit)
        return result
    except Exception as e:
        print(f"Error in preview: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/summary")
async def summary_endpoint(request: Dict[str, Any]):
    """Get data summary statistics (all columns or a specific column)"""
    try:
        data = request.get("data", [])
        column = request.get("column")  # Optional column name
        if not isinstance(data, list):
            raise HTTPException(status_code=400, detail="Data must be a list")
        
        if len(data) > config.MAX_ROWS:
            raise HTTPException(
                status_code=400,
                detail=f"Data exceeds maximum rows limit of {config.MAX_ROWS}"
            )
        
        result = get_summary(data=data, column=column)
        return result
    except Exception as e:
        print(f"Error in summary: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/create-derived-column")
async def create_derived_column_endpoint(request: CreateDerivedColumnRequest):
    """Create a new column from an expression"""
    try:
        if len(request.data) > config.MAX_ROWS:
            raise HTTPException(
                status_code=400,
                detail=f"Data exceeds maximum rows limit of {config.MAX_ROWS}"
            )
        
        result = create_derived_column(
            data=request.data,
            new_column_name=request.new_column_name,
            expression=request.expression
        )
        
        if result.get("errors") and len(result["errors"]) > 0:
            raise HTTPException(
                status_code=400,
                detail="; ".join(result["errors"])
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in create_derived_column: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/convert-type")
async def convert_type_endpoint(request: ConvertTypeRequest):
    """Convert column data type"""
    try:
        if len(request.data) > config.MAX_ROWS:
            raise HTTPException(
                status_code=400,
                detail=f"Data exceeds maximum rows limit of {config.MAX_ROWS}"
            )
        
        result = convert_type(
            data=request.data,
            column=request.column,
            target_type=request.target_type
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error in convert_type: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/train-model")
async def train_model_endpoint(request: TrainModelRequest):
    """Train a machine learning model"""
    try:
        # Validate data
        if not request.data or len(request.data) == 0:
            raise HTTPException(
                status_code=400,
                detail="Data is empty or not provided"
            )
        
        if len(request.data) > config.MAX_ROWS:
            raise HTTPException(
                status_code=400,
                detail=f"Data exceeds maximum rows limit of {config.MAX_ROWS}"
            )
        
        # Validate features list
        if not request.features or len(request.features) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one feature must be specified"
            )
        
        # Check for duplicate features
        if len(request.features) != len(set(request.features)):
            raise HTTPException(
                status_code=400,
                detail="Duplicate features found in features list"
            )
        
        # Validate target variable (required for supervised models, optional for unsupervised)
        unsupervised_models = ["kmeans", "dbscan", "hierarchical_clustering", "pca", "tsne", "umap"]
        is_unsupervised = request.model_type in unsupervised_models
        
        if not is_unsupervised:
            if not request.target_variable or not request.target_variable.strip():
                raise HTTPException(
                    status_code=400,
                    detail="Target variable is required for supervised learning models"
                )
            # Validate that target is not in features
            if request.target_variable in request.features:
                raise HTTPException(
                    status_code=400,
                    detail="Target variable cannot be in the features list"
                )
        
        # Train model based on type
        if request.model_type == "linear":
            result = train_linear_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "logistic":
            result = train_logistic_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "ridge":
            alpha = request.alpha if request.alpha is not None else 1.0
            result = train_ridge_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                alpha=alpha,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "lasso":
            alpha = request.alpha if request.alpha is not None else 1.0
            result = train_lasso_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                alpha=alpha,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "random_forest":
            n_estimators = request.n_estimators if request.n_estimators is not None else 100
            result = train_random_forest(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                n_estimators=n_estimators,
                max_depth=request.max_depth,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "decision_tree":
            result = train_decision_tree(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                max_depth=request.max_depth,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "gradient_boosting":
            n_estimators = request.n_estimators if request.n_estimators is not None else 100
            learning_rate = request.learning_rate if request.learning_rate is not None else 0.1
            max_depth = request.max_depth if request.max_depth is not None else 3
            result = train_gradient_boosting(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                n_estimators=n_estimators,
                learning_rate=learning_rate,
                max_depth=max_depth,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "elasticnet":
            alpha = request.alpha if request.alpha is not None else 1.0
            l1_ratio = request.l1_ratio if request.l1_ratio is not None else 0.5
            result = train_elasticnet(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                alpha=alpha,
                l1_ratio=l1_ratio,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "svm":
            kernel = request.kernel if request.kernel is not None else 'rbf'
            C = request.C if request.C is not None else 1.0
            result = train_svm(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                kernel=kernel,
                C=C,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "knn":
            n_neighbors = request.n_neighbors if request.n_neighbors is not None else 5
            result = train_knn(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                n_neighbors=n_neighbors,
                test_size=request.test_size,
                random_state=request.random_state
            )
        # Additional regression models
        elif request.model_type == "polynomial":
            degree = request.degree if request.degree is not None else 2
            result = train_polynomial_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                degree=degree,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "bayesian":
            result = train_bayesian_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "quantile":
            quantile = request.quantile if request.quantile is not None else 0.5
            alpha = request.alpha if request.alpha is not None else 1.0
            result = train_quantile_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                quantile=quantile,
                alpha=alpha,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "poisson":
            alpha = request.alpha if request.alpha is not None else 1.0
            result = train_poisson_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                alpha=alpha,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "gamma":
            alpha = request.alpha if request.alpha is not None else 1.0
            result = train_gamma_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                alpha=alpha,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "tweedie":
            power = request.power if request.power is not None else 0.0
            alpha = request.alpha if request.alpha is not None else 1.0
            result = train_tweedie_regression(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                power=power,
                alpha=alpha,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "extra_trees":
            n_estimators = request.n_estimators if request.n_estimators is not None else 100
            result = train_extra_trees(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                n_estimators=n_estimators,
                max_depth=request.max_depth,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "xgboost":
            n_estimators = request.n_estimators if request.n_estimators is not None else 100
            max_depth = request.max_depth if request.max_depth is not None else 3
            learning_rate = request.learning_rate if request.learning_rate is not None else 0.1
            result = train_xgboost(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "lightgbm":
            n_estimators = request.n_estimators if request.n_estimators is not None else 100
            max_depth = request.max_depth if request.max_depth is not None else -1
            learning_rate = request.learning_rate if request.learning_rate is not None else 0.1
            result = train_lightgbm(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "catboost":
            iterations = request.iterations if request.iterations is not None else 100
            depth = request.depth if request.depth is not None else 6
            learning_rate = request.learning_rate if request.learning_rate is not None else 0.1
            result = train_catboost(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                iterations=iterations,
                depth=depth,
                learning_rate=learning_rate,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "gaussian_process":
            result = train_gaussian_process(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "mlp":
            hidden_layer_sizes = tuple(request.hidden_layer_sizes) if request.hidden_layer_sizes else (100,)
            activation = request.activation if request.activation else 'relu'
            solver = request.solver if request.solver else 'adam'
            max_iter = request.max_iter if request.max_iter else 200
            result = train_mlp(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                hidden_layer_sizes=hidden_layer_sizes,
                activation=activation,
                solver=solver,
                alpha=request.alpha if request.alpha else 0.0001,
                learning_rate='constant',
                max_iter=max_iter,
                test_size=request.test_size,
                random_state=request.random_state
            )
        # Additional classification models
        elif request.model_type == "multinomial_logistic":
            result = train_multinomial_logistic(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type.startswith("naive_bayes_"):
            variant = request.model_type.replace("naive_bayes_", "")
            result = train_naive_bayes(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                variant=variant,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "lda":
            result = train_lda(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "qda":
            result = train_qda(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                test_size=request.test_size,
                random_state=request.random_state
            )
        # Unsupervised learning - Clustering
        elif request.model_type == "kmeans":
            n_clusters = request.n_clusters if request.n_clusters is not None else 3
            result = train_kmeans(
                data=request.data,
                features=request.features,
                n_clusters=n_clusters,
                random_state=request.random_state
            )
        elif request.model_type == "dbscan":
            eps = request.eps if request.eps is not None else 0.5
            min_samples = request.min_samples if request.min_samples is not None else 5
            result = train_dbscan(
                data=request.data,
                features=request.features,
                eps=eps,
                min_samples=min_samples
            )
        elif request.model_type == "hierarchical_clustering":
            n_clusters = request.n_clusters if request.n_clusters is not None else 3
            linkage = request.linkage if request.linkage else 'ward'
            result = train_hierarchical_clustering(
                data=request.data,
                features=request.features,
                n_clusters=n_clusters,
                linkage=linkage
            )
        # Unsupervised learning - Dimensionality Reduction
        elif request.model_type == "pca":
            result = train_pca(
                data=request.data,
                features=request.features,
                n_components=request.n_components
            )
        elif request.model_type == "tsne":
            n_components = request.n_components if request.n_components is not None else 2
            perplexity = request.perplexity if request.perplexity is not None else 30.0
            result = train_tsne(
                data=request.data,
                features=request.features,
                n_components=n_components,
                perplexity=perplexity,
                random_state=request.random_state
            )
        elif request.model_type == "umap":
            n_components = request.n_components if request.n_components is not None else 2
            min_dist = request.min_dist if request.min_dist is not None else 0.1
            n_neighbors = request.n_neighbors if request.n_neighbors is not None else 15
            result = train_umap(
                data=request.data,
                features=request.features,
                n_components=n_components,
                n_neighbors=n_neighbors,
                min_dist=min_dist,
                random_state=request.random_state
            )
        # Time series models
        elif request.model_type in ["arima", "sarima"]:
            order = tuple(request.order) if request.order and len(request.order) == 3 else (1, 1, 1)
            seasonal_order = tuple(request.seasonal_order) if request.seasonal_order and len(request.seasonal_order) == 4 else None
            result = train_arima(
                data=request.data,
                target_variable=request.target_variable,
                date_column=request.date_column,
                order=order,
                seasonal_order=seasonal_order
            )
        elif request.model_type == "exponential_smoothing":
            result = train_exponential_smoothing(
                data=request.data,
                target_variable=request.target_variable,
                date_column=request.date_column,
                trend=request.trend,
                seasonal=request.seasonal,
                seasonal_periods=request.seasonal_periods
            )
        elif request.model_type == "lstm":
            sequence_length = request.sequence_length if request.sequence_length is not None else 10
            lstm_units = request.lstm_units if request.lstm_units is not None else 50
            epochs = request.epochs if request.epochs is not None else 50
            result = train_lstm(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                sequence_length=sequence_length,
                lstm_units=lstm_units,
                epochs=epochs,
                test_size=request.test_size,
                random_state=request.random_state
            )
        elif request.model_type == "gru":
            sequence_length = request.sequence_length if request.sequence_length is not None else 10
            gru_units = request.gru_units if request.gru_units is not None else 50
            epochs = request.epochs if request.epochs is not None else 50
            result = train_gru(
                data=request.data,
                target_variable=request.target_variable,
                features=request.features,
                sequence_length=sequence_length,
                gru_units=gru_units,
                epochs=epochs,
                test_size=request.test_size,
                random_state=request.random_state
            )
        # Anomaly detection models
        elif request.model_type == "isolation_forest":
            contamination = request.contamination if request.contamination is not None else 0.1
            n_estimators = request.n_estimators if request.n_estimators is not None else 100
            result = train_isolation_forest(
                data=request.data,
                features=request.features,
                contamination=contamination,
                n_estimators=n_estimators,
                random_state=request.random_state
            )
        elif request.model_type == "one_class_svm":
            nu = request.nu if request.nu is not None else 0.1
            kernel = request.kernel if request.kernel is not None else 'rbf'
            result = train_one_class_svm(
                data=request.data,
                features=request.features,
                nu=nu,
                kernel=kernel,
                random_state=request.random_state
            )
        elif request.model_type == "local_outlier_factor":
            n_neighbors = request.n_neighbors if request.n_neighbors is not None else 20
            contamination = request.contamination if request.contamination is not None else 0.1
            result = train_local_outlier_factor(
                data=request.data,
                features=request.features,
                n_neighbors=n_neighbors,
                contamination=contamination
            )
        elif request.model_type == "elliptic_envelope":
            contamination = request.contamination if request.contamination is not None else 0.1
            result = train_elliptic_envelope(
                data=request.data,
                features=request.features,
                contamination=contamination,
                random_state=request.random_state
            )
        # Recommendation systems
        elif request.model_type == "matrix_factorization":
            if not request.user_column or not request.item_column or not request.rating_column:
                raise HTTPException(
                    status_code=400,
                    detail="user_column, item_column, and rating_column are required for matrix factorization"
                )
            n_factors = request.n_factors if request.n_factors is not None else 50
            n_epochs = request.n_epochs if request.n_epochs is not None else 20
            learning_rate = request.learning_rate if request.learning_rate is not None else 0.01
            regularization = request.regularization if request.regularization is not None else 0.1
            result = train_matrix_factorization(
                data=request.data,
                user_column=request.user_column,
                item_column=request.item_column,
                rating_column=request.rating_column,
                n_factors=n_factors,
                n_epochs=n_epochs,
                learning_rate=learning_rate,
                regularization=regularization
            )
        # Survival analysis
        elif request.model_type == "cox_proportional_hazards":
            if not request.duration_column or not request.event_column:
                raise HTTPException(
                    status_code=400,
                    detail="duration_column and event_column are required for Cox Proportional Hazards"
                )
            result = train_cox_proportional_hazards(
                data=request.data,
                duration_column=request.duration_column,
                event_column=request.event_column,
                features=request.features
            )
        elif request.model_type == "kaplan_meier":
            if not request.duration_column or not request.event_column:
                raise HTTPException(
                    status_code=400,
                    detail="duration_column and event_column are required for Kaplan-Meier"
                )
            result = train_kaplan_meier(
                data=request.data,
                duration_column=request.duration_column,
                event_column=request.event_column,
                group_column=request.group_column
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported model type: {request.model_type}"
            )
        
        return result
    except ValueError as e:
        print(f"ValueError in train_model: {str(e)}")
        print(f"Request details: model_type={request.model_type}, target_variable={request.target_variable}, features={request.features}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in train_model: {traceback.format_exc()}")
        print(f"Request details: model_type={request.model_type}, target_variable={request.target_variable}, features={request.features}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    print(f"Unhandled exception: {traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"}
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=config.HOST,
        port=config.PORT,
        reload=True
    )

