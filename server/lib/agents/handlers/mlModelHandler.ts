import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { trainMLModel } from '../../dataOps/pythonService.js';
import { findMatchingColumn } from '../utils/columnMatcher.js';
import { ChartSpec } from '../../../shared/schema.js';

/**
 * ML Model Handler
 * Handles machine learning model building requests
 */
export class MLModelHandler extends BaseHandler {
  canHandle(intent: AnalysisIntent): boolean {
    return intent.type === 'ml_model';
  }

  async handle(intent: AnalysisIntent, context: HandlerContext): Promise<HandlerResponse> {
    console.log('🤖 MLModelHandler processing intent:', intent.type);
    
    const validation = this.validateData(intent, context);
    if (!validation.valid && validation.errors.some(e => e.includes('No data'))) {
      return this.createErrorResponse(
        validation.errors.join(', '),
        intent,
        validation.suggestions
      );
    }

    // Extract model type
    let modelType: 'linear' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' = 'linear';
    
    if (intent.modelType) {
      modelType = intent.modelType;
    } else {
      // Try to extract from question
      const question = (intent.originalQuestion || intent.customRequest || '').toLowerCase();
      if (question.includes('logistic')) {
        modelType = 'logistic';
      } else if (question.includes('ridge')) {
        modelType = 'ridge';
      } else if (question.includes('lasso')) {
        modelType = 'lasso';
      } else if (question.includes('random forest') || question.includes('randomforest')) {
        modelType = 'random_forest';
      } else if (question.includes('decision tree') || question.includes('decisiontree')) {
        modelType = 'decision_tree';
      }
    }

    // Extract target variable
    let targetVariable = intent.targetVariable;
    if (!targetVariable) {
      // Try to extract from question
      const question = intent.originalQuestion || intent.customRequest || '';
      const targetMatch = question.match(/(?:choosing|predicting|target|dependent variable|target variable)\s+([a-zA-Z0-9_]+(?:\s+[a-zA-Z0-9_]+)*)/i);
      if (targetMatch && targetMatch[1]) {
        targetVariable = targetMatch[1].trim();
      }
    }

    if (!targetVariable) {
      const allColumns = context.summary.columns.map(c => c.name);
      return {
        answer: `I need to know which variable you'd like to use as the target (dependent variable). For example: 'Build a linear model choosing x as target variable and a, b, c as independent variables'`,
        requiresClarification: true,
        suggestions: allColumns.slice(0, 5),
      };
    }

    // Find matching target column
    const allColumns = context.summary.columns.map(c => c.name);
    const targetCol = findMatchingColumn(targetVariable, allColumns);

    if (!targetCol) {
      const suggestions = this.findSimilarColumns(targetVariable, context.summary);
      return {
        answer: `I couldn't find a column matching "${targetVariable}". ${suggestions.length > 0 ? `Did you mean: ${suggestions.join(', ')}?` : `Available columns: ${allColumns.slice(0, 5).join(', ')}${allColumns.length > 5 ? '...' : ''}`}`,
        requiresClarification: true,
        suggestions,
      };
    }

    // Extract features (independent variables)
    let features: string[] = [];
    
    if (intent.variables && intent.variables.length > 0) {
      features = intent.variables;
    } else {
      // Try to extract from question
      const question = intent.originalQuestion || intent.customRequest || '';
      
      // Pattern 1: "a, b, c, d, & e variables as independent"
      const featuresMatch1 = question.match(/(?:and|using|with|features|independent variables|predictors?)\s+([a-zA-Z0-9_]+(?:\s*[,\s&]+\s*[a-zA-Z0-9_]+)*)/i);
      if (featuresMatch1 && featuresMatch1[1]) {
        features = featuresMatch1[1]
          .split(/[,\s&]+/)
          .map(f => f.trim())
          .filter(f => f.length > 0);
      }
      
      // Pattern 2: "a, b, c, d, & e" after "independent variables"
      if (features.length === 0) {
        const featuresMatch2 = question.match(/independent\s+variables?\s+(?:are\s+)?([a-zA-Z0-9_]+(?:\s*[,\s&]+\s*[a-zA-Z0-9_]+)*)/i);
        if (featuresMatch2 && featuresMatch2[1]) {
          features = featuresMatch2[1]
            .split(/[,\s&]+/)
            .map(f => f.trim())
            .filter(f => f.length > 0);
        }
      }
    }

    if (features.length === 0) {
      // Suggest all numeric columns except target
      const numericColumns = context.summary.numericColumns.filter(col => col !== targetCol);
      return {
        answer: `I need to know which variables to use as independent variables (features). For example: 'Build a linear model choosing ${targetCol} as target variable and ${numericColumns.slice(0, 3).join(', ')} as independent variables'`,
        requiresClarification: true,
        suggestions: numericColumns.slice(0, 5),
      };
    }

    // Match feature names to actual columns
    const matchedFeatures = features
      .map(f => findMatchingColumn(f, allColumns))
      .filter((f): f is string => f !== null && f !== targetCol);

    if (matchedFeatures.length === 0) {
      const numericColumns = context.summary.numericColumns.filter(col => col !== targetCol);
      return {
        answer: `I couldn't match any of the specified features to columns in your dataset. Available numeric columns (excluding target): ${numericColumns.slice(0, 5).join(', ')}${numericColumns.length > 5 ? '...' : ''}`,
        requiresClarification: true,
        suggestions: numericColumns.slice(0, 5),
      };
    }

    // Remove duplicates
    const uniqueFeatures = Array.from(new Set(matchedFeatures));

    console.log(`🤖 Training ${modelType} model: target="${targetCol}", features=[${uniqueFeatures.join(', ')}]`);

    try {
      // Train the model
      const modelResult = await trainMLModel(
        context.data,
        modelType,
        targetCol,
        uniqueFeatures
      );

      // Format response
      const answer = this.formatModelResponse(modelResult, modelType, targetCol, uniqueFeatures);
      
      // Generate charts if applicable
      const charts = this.generateModelCharts(modelResult, targetCol, uniqueFeatures);

      return {
        answer,
        charts,
        operationResult: modelResult,
      };
    } catch (error) {
      console.error('ML model training error:', error);
      return this.createErrorResponse(
        error instanceof Error ? error : new Error(String(error)),
        intent,
        this.findSimilarColumns(targetVariable, context.summary)
      );
    }
  }

  private formatModelResponse(
    result: any,
    modelType: string,
    targetCol: string,
    features: string[]
  ): string {
    let answer = `I've successfully trained a ${modelType.replace('_', ' ')} model.\n\n`;
    
    answer += `**Model Summary:**\n`;
    answer += `- Target Variable: ${targetCol}\n`;
    answer += `- Features: ${features.join(', ')}\n`;
    answer += `- Training Samples: ${result.n_train}\n`;
    answer += `- Test Samples: ${result.n_test}\n\n`;

    // Add metrics
    answer += `**Model Performance:**\n`;
    
    if (result.task_type === 'regression') {
      const testMetrics = result.metrics.test;
      answer += `- R² Score: ${testMetrics.r2_score?.toFixed(4) || 'N/A'}\n`;
      answer += `- RMSE: ${testMetrics.rmse?.toFixed(4) || 'N/A'}\n`;
      answer += `- MAE: ${testMetrics.mae?.toFixed(4) || 'N/A'}\n`;
      
      if (result.metrics.cross_validation?.mean_r2) {
        answer += `- Cross-Validation R² (mean): ${result.metrics.cross_validation.mean_r2.toFixed(4)}\n`;
      }
    } else {
      const testMetrics = result.metrics.test;
      answer += `- Accuracy: ${(testMetrics.accuracy * 100)?.toFixed(2) || 'N/A'}%\n`;
      answer += `- Precision: ${(testMetrics.precision * 100)?.toFixed(2) || 'N/A'}%\n`;
      answer += `- Recall: ${(testMetrics.recall * 100)?.toFixed(2) || 'N/A'}%\n`;
      answer += `- F1 Score: ${(testMetrics.f1_score * 100)?.toFixed(2) || 'N/A'}%\n`;
      
      if (result.metrics.cross_validation?.mean_accuracy) {
        answer += `- Cross-Validation Accuracy (mean): ${(result.metrics.cross_validation.mean_accuracy * 100).toFixed(2)}%\n`;
      }
    }

    answer += `\n`;

    // Add coefficients for linear models
    if (result.coefficients) {
      answer += `**Model Coefficients:**\n`;
      answer += `- Intercept: ${typeof result.coefficients.intercept === 'number' ? result.coefficients.intercept.toFixed(4) : 'N/A'}\n`;
      
      if (result.coefficients.features) {
        const featureCoefs = Object.entries(result.coefficients.features)
          .sort((a, b) => {
            const aVal = typeof a[1] === 'number' ? Math.abs(a[1]) : 0;
            const bVal = typeof b[1] === 'number' ? Math.abs(b[1]) : 0;
            return bVal - aVal;
          });
        
        for (const [feature, coef] of featureCoefs) {
          const coefValue = typeof coef === 'number' ? coef.toFixed(4) : 'N/A';
          answer += `- ${feature}: ${coefValue}\n`;
        }
      }
      answer += `\n`;
    }

    // Add feature importance for tree-based models
    if (result.feature_importance) {
      answer += `**Feature Importance:**\n`;
      const importanceEntries = Object.entries(result.feature_importance)
        .sort((a, b) => (b[1] as number) - (a[1] as number));
      
      for (const [feature, importance] of importanceEntries) {
        answer += `- ${feature}: ${(importance as number).toFixed(4)}\n`;
      }
      answer += `\n`;
    }

    // Add insights
    answer += `**Key Insights:**\n`;
    if (result.task_type === 'regression') {
      const r2 = result.metrics.test.r2_score;
      if (r2 > 0.8) {
        answer += `- The model explains ${(r2 * 100).toFixed(1)}% of the variance, indicating excellent fit.\n`;
      } else if (r2 > 0.6) {
        answer += `- The model explains ${(r2 * 100).toFixed(1)}% of the variance, indicating good fit.\n`;
      } else if (r2 > 0.4) {
        answer += `- The model explains ${(r2 * 100).toFixed(1)}% of the variance, indicating moderate fit.\n`;
      } else {
        answer += `- The model explains ${(r2 * 100).toFixed(1)}% of the variance, indicating poor fit. Consider feature engineering or different model types.\n`;
      }
    } else {
      const accuracy = result.metrics.test.accuracy;
      if (accuracy > 0.9) {
        answer += `- The model achieves ${(accuracy * 100).toFixed(1)}% accuracy, indicating excellent performance.\n`;
      } else if (accuracy > 0.7) {
        answer += `- The model achieves ${(accuracy * 100).toFixed(1)}% accuracy, indicating good performance.\n`;
      } else {
        answer += `- The model achieves ${(accuracy * 100).toFixed(1)}% accuracy. Consider feature engineering or different model types.\n`;
      }
    }

    return answer;
  }

  private generateModelCharts(
    result: any,
    targetCol: string,
    features: string[]
  ): ChartSpec[] | undefined {
    const charts: ChartSpec[] = [];

    // Feature importance chart for tree-based models
    if (result.feature_importance) {
      const importanceData = Object.entries(result.feature_importance)
        .sort((a, b) => (b[1] as number) - (a[1] as number))
        .map(([feature, importance]) => ({
          variable: feature,
          importance: importance as number,
        }));

      charts.push({
        type: 'bar',
        title: `Feature Importance - ${result.model_type.replace('_', ' ')}`,
        x: 'variable',
        y: 'importance',
        aggregate: 'none',
        data: importanceData,
        xLabel: 'Feature',
        yLabel: 'Importance',
      });
    }

    // Coefficients chart for linear models
    if (result.coefficients && result.coefficients.features) {
      const coefData = Object.entries(result.coefficients.features)
        .map(([feature, coef]) => ({
          variable: feature,
          coefficient: typeof coef === 'number' ? coef : 0,
        }))
        .sort((a, b) => Math.abs(b.coefficient) - Math.abs(a.coefficient));

      charts.push({
        type: 'bar',
        title: `Model Coefficients - ${result.model_type.replace('_', ' ')}`,
        x: 'variable',
        y: 'coefficient',
        aggregate: 'none',
        data: coefData,
        xLabel: 'Feature',
        yLabel: 'Coefficient',
      });
    }

    return charts.length > 0 ? charts : undefined;
  }
}

