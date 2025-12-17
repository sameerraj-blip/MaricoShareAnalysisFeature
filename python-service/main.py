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
    train_knn
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
    model_type: Literal["linear", "logistic", "ridge", "lasso", "random_forest", "decision_tree", "gradient_boosting", "elasticnet", "svm", "knn"]
    target_variable: str
    features: List[str]
    test_size: float = Field(default=0.2, ge=0.1, le=0.5)
    random_state: int = Field(default=42)
    alpha: Optional[float] = Field(default=None, ge=0.0)  # For Ridge/Lasso/ElasticNet
    l1_ratio: Optional[float] = Field(default=None, ge=0.0, le=1.0)  # For ElasticNet
    n_estimators: Optional[int] = Field(default=None, ge=1)  # For Random Forest/Gradient Boosting
    max_depth: Optional[int] = Field(default=None, ge=1)  # For Random Forest/Decision Tree/Gradient Boosting
    learning_rate: Optional[float] = Field(default=None, gt=0.0)  # For Gradient Boosting
    kernel: Optional[str] = Field(default=None)  # For SVM (rbf, linear, poly, sigmoid)
    C: Optional[float] = Field(default=None, gt=0.0)  # For SVM
    n_neighbors: Optional[int] = Field(default=None, ge=1)  # For KNN


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
        
        # Validate target variable
        if not request.target_variable or not request.target_variable.strip():
            raise HTTPException(
                status_code=400,
                detail="Target variable is required"
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

