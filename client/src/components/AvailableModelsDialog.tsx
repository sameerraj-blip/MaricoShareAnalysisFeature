import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Info } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";

interface ModelInfo {
  name: string;
  displayName: string;
  category: 'regression' | 'classification' | 'ensemble' | 'neural' | 'clustering' | 'dimensionality' | 'time_series' | 'anomaly' | 'survival';
  description: string;
}

const AVAILABLE_MODELS: ModelInfo[] = [
  // Regression Models
  { name: 'linear', displayName: 'Linear Regression', category: 'regression', description: 'Finds the best-fit line through data. Assumes linear relationships.' },
  { name: 'log_log', displayName: 'Log-Log Regression', category: 'regression', description: 'Applies log transformation to both target and features. Useful for elasticity analysis and multiplicative relationships.' },
  { name: 'ridge', displayName: 'Ridge Regression', category: 'regression', description: 'Linear regression with L2 regularization to prevent overfitting.' },
  { name: 'lasso', displayName: 'Lasso Regression', category: 'regression', description: 'Linear regression with L1 regularization for feature selection.' },
  { name: 'elasticnet', displayName: 'ElasticNet', category: 'regression', description: 'Combines L1 and L2 regularization. Balances feature selection with coefficient shrinkage.' },
  { name: 'polynomial', displayName: 'Polynomial Regression', category: 'regression', description: 'Models non-linear relationships using polynomial features.' },
  { name: 'bayesian', displayName: 'Bayesian Ridge', category: 'regression', description: 'Ridge regression with Bayesian inference for uncertainty estimation.' },
  { name: 'quantile', displayName: 'Quantile Regression', category: 'regression', description: 'Estimates conditional quantiles, useful for robust predictions.' },
  { name: 'poisson', displayName: 'Poisson Regression', category: 'regression', description: 'For count data with Poisson distribution assumption.' },
  { name: 'gamma', displayName: 'Gamma Regression', category: 'regression', description: 'For positive continuous data with gamma distribution.' },
  { name: 'tweedie', displayName: 'Tweedie Regression', category: 'regression', description: 'Generalized linear model for various distributions.' },
  
  // Classification Models
  { name: 'logistic', displayName: 'Logistic Regression', category: 'classification', description: 'Binary classification using the logistic function.' },
  { name: 'multinomial_logistic', displayName: 'Multinomial Logistic', category: 'classification', description: 'Multi-class classification using logistic regression.' },
  { name: 'svm', displayName: 'Support Vector Machine', category: 'classification', description: 'Finds optimal hyperplane for classification or regression.' },
  { name: 'knn', displayName: 'K-Nearest Neighbors', category: 'classification', description: 'Makes predictions based on k closest training examples.' },
  { name: 'naive_bayes_gaussian', displayName: 'Gaussian Naive Bayes', category: 'classification', description: 'Probabilistic classifier assuming Gaussian features.' },
  { name: 'naive_bayes_multinomial', displayName: 'Multinomial Naive Bayes', category: 'classification', description: 'For discrete count data (e.g., text classification).' },
  { name: 'naive_bayes_bernoulli', displayName: 'Bernoulli Naive Bayes', category: 'classification', description: 'For binary/boolean features.' },
  { name: 'lda', displayName: 'Linear Discriminant Analysis', category: 'classification', description: 'Dimensionality reduction and classification technique.' },
  { name: 'qda', displayName: 'Quadratic Discriminant Analysis', category: 'classification', description: 'Non-linear variant of LDA.' },
  
  // Ensemble Models
  { name: 'random_forest', displayName: 'Random Forest', category: 'ensemble', description: 'Ensemble of decision trees. Robust and handles non-linear relationships.' },
  { name: 'decision_tree', displayName: 'Decision Tree', category: 'ensemble', description: 'Tree-like model of decisions. Highly interpretable.' },
  { name: 'gradient_boosting', displayName: 'Gradient Boosting', category: 'ensemble', description: 'Sequential ensemble that corrects previous errors.' },
  { name: 'extra_trees', displayName: 'Extra Trees', category: 'ensemble', description: 'Extremely randomized trees for variance reduction.' },
  { name: 'xgboost', displayName: 'XGBoost', category: 'ensemble', description: 'Optimized gradient boosting with regularization.' },
  { name: 'lightgbm', displayName: 'LightGBM', category: 'ensemble', description: 'Fast gradient boosting framework with leaf-wise growth.' },
  { name: 'catboost', displayName: 'CatBoost', category: 'ensemble', description: 'Gradient boosting optimized for categorical features.' },
  
  // Neural Networks
  { name: 'mlp', displayName: 'Multi-Layer Perceptron', category: 'neural', description: 'Feedforward neural network for complex non-linear patterns.' },
  { name: 'gaussian_process', displayName: 'Gaussian Process', category: 'neural', description: 'Non-parametric Bayesian approach for regression.' },
  
  // Clustering
  { name: 'kmeans', displayName: 'K-Means', category: 'clustering', description: 'Partitions data into k clusters based on centroids.' },
  { name: 'dbscan', displayName: 'DBSCAN', category: 'clustering', description: 'Density-based clustering for arbitrary-shaped clusters.' },
  { name: 'hierarchical_clustering', displayName: 'Hierarchical Clustering', category: 'clustering', description: 'Builds tree of clusters using linkage criteria.' },
  
  // Dimensionality Reduction
  { name: 'pca', displayName: 'Principal Component Analysis', category: 'dimensionality', description: 'Reduces dimensions while preserving variance.' },
  { name: 'tsne', displayName: 't-SNE', category: 'dimensionality', description: 'Non-linear dimensionality reduction for visualization.' },
  { name: 'umap', displayName: 'UMAP', category: 'dimensionality', description: 'Uniform Manifold Approximation for dimensionality reduction.' },
  
  // Time Series
  { name: 'arima', displayName: 'ARIMA', category: 'time_series', description: 'AutoRegressive Integrated Moving Average for time series.' },
  { name: 'sarima', displayName: 'SARIMA', category: 'time_series', description: 'Seasonal ARIMA for time series with seasonality.' },
  { name: 'exponential_smoothing', displayName: 'Exponential Smoothing', category: 'time_series', description: 'Forecasting method using weighted averages.' },
  { name: 'lstm', displayName: 'LSTM', category: 'time_series', description: 'Long Short-Term Memory network for sequential data.' },
  { name: 'gru', displayName: 'GRU', category: 'time_series', description: 'Gated Recurrent Unit for time series prediction.' },
  
  // Anomaly Detection
  { name: 'isolation_forest', displayName: 'Isolation Forest', category: 'anomaly', description: 'Isolates anomalies using random forests.' },
  { name: 'one_class_svm', displayName: 'One-Class SVM', category: 'anomaly', description: 'Detects outliers using support vector methods.' },
  { name: 'local_outlier_factor', displayName: 'Local Outlier Factor', category: 'anomaly', description: 'Measures local deviation from neighbors.' },
  { name: 'elliptic_envelope', displayName: 'Elliptic Envelope', category: 'anomaly', description: 'Fits robust covariance to detect outliers.' },
  
  // Other
  { name: 'matrix_factorization', displayName: 'Matrix Factorization', category: 'dimensionality', description: 'For collaborative filtering and recommendation systems.' },
  { name: 'cox_proportional_hazards', displayName: 'Cox Proportional Hazards', category: 'survival', description: 'Survival analysis for time-to-event data.' },
  { name: 'kaplan_meier', displayName: 'Kaplan-Meier', category: 'survival', description: 'Non-parametric survival curve estimation.' },
];

const CATEGORY_LABELS: Record<string, string> = {
  regression: 'Regression',
  classification: 'Classification',
  ensemble: 'Ensemble',
  neural: 'Neural Networks',
  clustering: 'Clustering',
  dimensionality: 'Dimensionality Reduction',
  time_series: 'Time Series',
  anomaly: 'Anomaly Detection',
  survival: 'Survival Analysis',
};

const CATEGORY_COLORS: Record<string, string> = {
  regression: 'bg-blue-100 text-blue-800',
  classification: 'bg-green-100 text-green-800',
  ensemble: 'bg-purple-100 text-purple-800',
  neural: 'bg-orange-100 text-orange-800',
  clustering: 'bg-pink-100 text-pink-800',
  dimensionality: 'bg-yellow-100 text-yellow-800',
  time_series: 'bg-cyan-100 text-cyan-800',
  anomaly: 'bg-red-100 text-red-800',
  survival: 'bg-indigo-100 text-indigo-800',
};

export function AvailableModelsDialog() {
  const modelsByCategory = AVAILABLE_MODELS.reduce((acc, model) => {
    if (!acc[model.category]) {
      acc[model.category] = [];
    }
    acc[model.category].push(model);
    return acc;
  }, {} as Record<string, ModelInfo[]>);

  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="gap-2">
          <Info className="w-4 h-4" />
          Available Models
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-4xl max-h-[80vh]">
        <DialogHeader>
          <DialogTitle>Available Machine Learning Models</DialogTitle>
          <DialogDescription>
            Browse all {AVAILABLE_MODELS.length} models available in the system. Click on a model to see its description.
          </DialogDescription>
        </DialogHeader>
        <ScrollArea className="h-[60vh] pr-4">
          <div className="space-y-6">
            {Object.entries(modelsByCategory).map(([category, models]) => (
              <div key={category} className="space-y-3">
                <div className="flex items-center gap-2">
                  <h3 className="text-lg font-semibold">{CATEGORY_LABELS[category]}</h3>
                  <Badge variant="secondary" className={CATEGORY_COLORS[category]}>
                    {models.length} model{models.length !== 1 ? 's' : ''}
                  </Badge>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {models.map((model) => (
                    <div
                      key={model.name}
                      className="p-3 border rounded-lg hover:bg-accent transition-colors"
                    >
                      <div className="flex items-start justify-between gap-2 mb-2">
                        <h4 className="font-medium text-sm">{model.displayName}</h4>
                        <Badge variant="outline" className="text-xs">
                          {model.name}
                        </Badge>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        {model.description}
                      </p>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </ScrollArea>
        <div className="mt-4 p-3 bg-muted rounded-lg">
          <p className="text-sm text-muted-foreground">
            <strong>Tip:</strong> You can request any of these models by name. For example: 
            "Build a {AVAILABLE_MODELS[0].displayName} model" or "Train a {AVAILABLE_MODELS[1].name} model"
          </p>
        </div>
      </DialogContent>
    </Dialog>
  );
}

