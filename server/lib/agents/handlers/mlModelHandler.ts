import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { trainMLModel } from '../../dataOps/pythonService.js';
import { findMatchingColumn } from '../utils/columnMatcher.js';
import { ChartSpec } from '../../../shared/schema.js';
import { openai } from '../../openai.js';

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
    
    // Check if this is an advice question (should not train model, just provide advice)
    const userQuestion = intent.customRequest || intent.originalQuestion || '';
    if (this.isAdviceQuestion(userQuestion)) {
      console.log('💡 Detected advice question in MLModelHandler, providing simple response');
      return this.handleAdviceQuestion(userQuestion, context);
    }
    
    const validation = this.validateData(intent, context);
    if (!validation.valid && validation.errors.some(e => e.includes('No data'))) {
      return this.createErrorResponse(
        validation.errors.join(', '),
        intent,
        validation.suggestions
      );
    }

    // Extract model type
    let modelType: 'linear' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn' = 'linear';
    
    if (intent.modelType) {
      modelType = intent.modelType as typeof modelType;
    } else {
      // Try to extract from question
      const questionLower = (intent.originalQuestion || intent.customRequest || '').toLowerCase();
      if (questionLower.includes('logistic')) {
        modelType = 'logistic';
      } else if (questionLower.includes('ridge')) {
        modelType = 'ridge';
      } else if (questionLower.includes('lasso')) {
        modelType = 'lasso';
      } else if (questionLower.includes('random forest') || questionLower.includes('randomforest')) {
        modelType = 'random_forest';
      } else if (questionLower.includes('decision tree') || questionLower.includes('decisiontree')) {
        modelType = 'decision_tree';
      } else if (questionLower.includes('gradient boosting') || questionLower.includes('gradientboosting') || questionLower.includes('gbm') || questionLower.includes('xgboost')) {
        modelType = 'gradient_boosting';
      } else if (questionLower.includes('elastic net') || questionLower.includes('elasticnet')) {
        modelType = 'elasticnet';
      } else if (questionLower.includes('svm') || questionLower.includes('support vector')) {
        modelType = 'svm';
      } else if (questionLower.includes('knn') || questionLower.includes('k-nearest') || questionLower.includes('k nearest') || questionLower.includes('nearest neighbor')) {
        modelType = 'knn';
      }
    }

    // Check if this is a follow-up question about modeling (e.g., "test alternative features", "improve accuracy")
    const questionText = (intent.originalQuestion || intent.customRequest || '').toLowerCase();
    const isFollowUpQuestion = /\b(test|try|alternative|different|other|improve|better|change|switch|more)\s*(features?|variables?|accuracy|metrics?|model)\b/i.test(questionText) ||
                               /\b(should we|can we|let's|what if)\b/i.test(questionText);
    
    // Extract target variable
    let targetVariable = intent.targetVariable;
    if (!targetVariable) {
      // Try to extract from question
      const questionText = intent.originalQuestion || intent.customRequest || '';
      const targetMatch = questionText.match(/(?:choosing|predicting|target|dependent variable|target variable)\s+([a-zA-Z0-9_]+(?:\s+[a-zA-Z0-9_]+)*)/i);
      if (targetMatch && targetMatch[1]) {
        targetVariable = targetMatch[1].trim();
      }
    }
    
    // If no target and this looks like a follow-up, try to extract from chat history
    if (!targetVariable && isFollowUpQuestion && context.chatHistory && context.chatHistory.length > 0) {
      const previousContext = this.extractModelContextFromHistory(context.chatHistory, context.summary.columns.map(c => c.name));
      if (previousContext.target) {
        targetVariable = previousContext.target;
        console.log(`📌 Extracted target from chat history: ${targetVariable}`);
      }
    }

    if (!targetVariable) {
      const allColumns = context.summary.columns.map(c => c.name);
      
      // If this is a follow-up question, give a more contextual response
      if (isFollowUpQuestion) {
        return {
          answer: `I'd be happy to help you test alternative features! To do that, I need to know which variable you're trying to predict. Could you specify the target variable? For example: "Test alternative features for predicting PA TOM"`,
          requiresClarification: true,
          suggestions: allColumns.slice(0, 5),
        };
      }
      
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

    // If no features and this is a follow-up, try to get from history or suggest alternatives
    if (features.length === 0 && isFollowUpQuestion && context.chatHistory && context.chatHistory.length > 0) {
      const previousContext = this.extractModelContextFromHistory(context.chatHistory, allColumns);
      if (previousContext.features.length > 0) {
        // User wants to test alternative features - suggest different ones
        const numericColumns = context.summary.numericColumns.filter(col => col !== targetCol);
        const unusedFeatures = numericColumns.filter(col => !previousContext.features.includes(col));
        
        if (unusedFeatures.length > 0) {
          // Automatically use different features
          features = unusedFeatures.slice(0, Math.max(3, previousContext.features.length));
          console.log(`📌 Testing alternative features: ${features.join(', ')} (previously used: ${previousContext.features.join(', ')})`);
        } else {
          features = previousContext.features; // Use same features if no alternatives
        }
        
        // Also get model type from history if not specified
        if (!intent.modelType && previousContext.modelType) {
          modelType = previousContext.modelType as typeof modelType;
        }
      }
    }

    if (features.length === 0) {
      // Suggest all numeric columns except target
      const numericColumns = context.summary.numericColumns.filter(col => col !== targetCol);
      
      // For follow-up questions, be more helpful
      if (isFollowUpQuestion) {
        return {
          answer: `I'd be happy to test alternative features for predicting ${targetCol}! Here are some options you could try:\n\n${numericColumns.slice(0, 5).map(c => `- ${c}`).join('\n')}\n\nWhich features would you like to test? Or say "use all" to test with all available features.`,
          requiresClarification: true,
          suggestions: numericColumns.slice(0, 5),
        };
      }
      
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
      // Train the model using Python service (primary method - returns actual results)
      console.log('🤖 Using Python service to train model...');
      const modelResult = await trainMLModel(
        context.data,
        modelType,
        targetCol,
        uniqueFeatures
      );

      // Format response with full model results
      const answer = this.formatModelResponse(modelResult, modelType, targetCol, uniqueFeatures);
      
      // Generate charts for visualization
      const charts = this.generateModelCharts(modelResult, targetCol, uniqueFeatures);

      return {
        answer,
        charts,
        operationResult: modelResult,
      };
    } catch (pythonError) {
      console.error('Python service failed, trying GPT-4o code generation fallback:', pythonError);
      
      // Fallback to GPT-4o code generation if Python service fails
      try {
        console.log('🔄 Falling back to GPT-4o for code generation...');
        const { code, explanation } = await this.generateModelCode(
          modelType,
          targetCol,
          uniqueFeatures,
          context
        );

        // Format the response with explanation and code
        const answer = this.formatCodeResponse(explanation, code, modelType, targetCol, uniqueFeatures);

        return {
          answer,
          charts: undefined,
          operationResult: { 
            type: 'code_generation',
            model_type: modelType,
            target: targetCol,
            features: uniqueFeatures 
          },
        };
      } catch (gptError) {
        console.error('GPT-4o code generation also failed:', gptError);
        return this.createErrorResponse(
          pythonError instanceof Error ? pythonError : new Error(String(pythonError)),
          intent,
          this.findSimilarColumns(targetVariable, context.summary)
        );
      }
    }
  }

  /**
   * Format the response with code block, explanation, and usage instructions
   */
  private formatCodeResponse(
    explanation: string,
    code: string,
    modelType: string,
    targetCol: string,
    features: string[]
  ): string {
    let response = explanation;
    
    response += `### Python Code\n\n`;
    response += `\`\`\`python\n${code}\n\`\`\`\n\n`;
    
    response += `### Next Steps\n`;
    response += `- Copy the code above and save it to a Python file\n`;
    response += `- Update the data file path to point to your actual CSV file\n`;
    response += `- Run the script to train your ${modelType.replace('_', ' ')} model\n`;
    response += `- Review the metrics and visualizations to assess model performance\n\n`;
    
    response += `**Need the model trained on the server instead?** Let me know and I can run it for you directly.\n`;
    
    return response;
  }

  /**
   * Generate Python code for ML model using GPT-4o
   */
  private async generateModelCode(
    modelType: string,
    targetCol: string,
    features: string[],
    context: HandlerContext
  ): Promise<{ code: string; explanation: string }> {
    // Build data schema information
    const columnInfo = context.summary.columns.map(c => ({
      name: c.name,
      type: context.summary.numericColumns.includes(c.name) ? 'numeric' : 
            context.summary.dateColumns.includes(c.name) ? 'date' : 'string'
    }));

    // Get sample data (first 3 rows) for context
    const sampleData = context.data.slice(0, 3).map(row => {
      const sample: Record<string, any> = {};
      for (const col of [...features, targetCol]) {
        if (row[col] !== undefined) {
          sample[col] = row[col];
        }
      }
      return sample;
    });

    const modelTypeDisplay = modelType.replace('_', ' ');
    
    // Determine if this is a classification or regression task
    const targetColumnInfo = columnInfo.find(c => c.name === targetCol);
    const isClassification = modelType === 'logistic' || 
      (targetColumnInfo?.type !== 'numeric');

    const prompt = `You are a Python machine learning expert. Generate complete, runnable Python code for training a ${modelTypeDisplay} model.

## Task Details
- Model Type: ${modelTypeDisplay}
- Target Variable: ${targetCol}
- Feature Variables: ${features.join(', ')}
- Task Type: ${isClassification ? 'Classification' : 'Regression'}

## Data Schema
Columns: ${JSON.stringify(columnInfo, null, 2)}

## Sample Data (first 3 rows, relevant columns only)
${JSON.stringify(sampleData, null, 2)}

## Requirements
Generate a complete Python script that:
1. Imports all necessary libraries (pandas, sklearn, matplotlib, seaborn)
2. Loads data from a CSV file (use placeholder path 'your_data.csv')
3. Handles missing values appropriately
4. Prepares features and target variable
5. Splits data into train/test sets (80/20)
6. Trains a ${modelTypeDisplay} model
7. Evaluates the model with appropriate metrics:
   ${isClassification ? '- Accuracy, Precision, Recall, F1-Score, Confusion Matrix' : '- R² Score, RMSE, MAE'}
8. Creates visualizations:
   ${isClassification ? '- Confusion matrix heatmap' : '- Actual vs Predicted scatter plot, Residual plot'}
   - Feature importance or coefficients bar chart
9. Prints model summary and insights
10. Includes cross-validation (5-fold)

## Code Style
- Add clear comments explaining each step
- Use descriptive variable names
- Handle potential errors gracefully
- Make the code copy-paste ready

Return ONLY the Python code, no explanations before or after.`;

    try {
      const response = await openai.chat.completions.create({
        model: 'gpt-4o',
        messages: [
          {
            role: 'system',
            content: 'You are a Python machine learning expert. Generate clean, well-documented, production-ready Python code. Return ONLY the code with no additional text.'
          },
          {
            role: 'user',
            content: prompt
          }
        ],
        temperature: 0.3,
        max_tokens: 4000,
      });

      const code = response.choices[0]?.message?.content?.trim() || '';
      
      // Clean up code - remove markdown code blocks if present
      let cleanCode = code;
      if (cleanCode.startsWith('```python')) {
        cleanCode = cleanCode.slice(9);
      } else if (cleanCode.startsWith('```')) {
        cleanCode = cleanCode.slice(3);
      }
      if (cleanCode.endsWith('```')) {
        cleanCode = cleanCode.slice(0, -3);
      }
      cleanCode = cleanCode.trim();

      // Generate explanation
      const explanation = this.generateCodeExplanation(modelType, targetCol, features, isClassification);

      return { code: cleanCode, explanation };
    } catch (error) {
      console.error('GPT-4o code generation error:', error);
      throw new Error(`Failed to generate model code: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Generate explanation for the generated code
   */
  private generateCodeExplanation(
    modelType: string,
    targetCol: string,
    features: string[],
    isClassification: boolean
  ): string {
    const modelTypeDisplay = modelType.replace('_', ' ');
    
    let explanation = `## ${modelTypeDisplay.charAt(0).toUpperCase() + modelTypeDisplay.slice(1)} Model\n\n`;
    
    explanation += `**Target Variable:** ${targetCol}\n`;
    explanation += `**Feature Variables:** ${features.join(', ')}\n`;
    explanation += `**Task Type:** ${isClassification ? 'Classification' : 'Regression'}\n\n`;
    
    explanation += `### Model Description\n`;
    switch (modelType) {
      case 'linear':
        explanation += `Linear Regression finds the best-fit line through the data by minimizing the sum of squared residuals. It assumes a linear relationship between features and target.\n\n`;
        break;
      case 'logistic':
        explanation += `Logistic Regression is used for binary classification. It models the probability of class membership using the logistic function.\n\n`;
        break;
      case 'ridge':
        explanation += `Ridge Regression adds L2 regularization to linear regression, which helps prevent overfitting by penalizing large coefficients.\n\n`;
        break;
      case 'lasso':
        explanation += `Lasso Regression adds L1 regularization, which can shrink some coefficients to zero, effectively performing feature selection.\n\n`;
        break;
      case 'random_forest':
        explanation += `Random Forest is an ensemble method that builds multiple decision trees and averages their predictions. It's robust to overfitting and handles non-linear relationships well.\n\n`;
        break;
      case 'decision_tree':
        explanation += `Decision Tree creates a tree-like model of decisions. It's interpretable but can overfit without proper pruning.\n\n`;
        break;
      case 'gradient_boosting':
        explanation += `Gradient Boosting builds models sequentially, with each new model correcting errors made by previous ones. It's powerful for both regression and classification tasks.\n\n`;
        break;
      case 'elasticnet':
        explanation += `ElasticNet combines L1 (Lasso) and L2 (Ridge) regularization. It balances feature selection with coefficient shrinkage, useful when dealing with correlated features.\n\n`;
        break;
      case 'svm':
        explanation += `Support Vector Machine finds the optimal hyperplane that separates classes (classification) or fits data (regression). It works well in high-dimensional spaces.\n\n`;
        break;
      case 'knn':
        explanation += `K-Nearest Neighbors makes predictions based on the k closest training examples. It's simple, non-parametric, and works well for both classification and regression.\n\n`;
        break;
    }
    
    explanation += `### How to Run\n`;
    explanation += `1. Save the code below to a file (e.g., \`train_model.py\`)\n`;
    explanation += `2. Replace \`'your_data.csv'\` with the actual path to your data file\n`;
    explanation += `3. Install required packages: \`pip install pandas scikit-learn matplotlib seaborn\`\n`;
    explanation += `4. Run: \`python train_model.py\`\n\n`;
    
    explanation += `### Expected Output\n`;
    if (isClassification) {
      explanation += `- Model accuracy and classification metrics\n`;
      explanation += `- Confusion matrix visualization\n`;
      explanation += `- Feature importance chart\n`;
    } else {
      explanation += `- R² score, RMSE, and MAE metrics\n`;
      explanation += `- Actual vs Predicted plot\n`;
      explanation += `- Residual analysis plot\n`;
      explanation += `- Feature coefficients or importance chart\n`;
    }
    explanation += `- Cross-validation results\n\n`;
    
    return explanation;
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
        .map(([feature, coef]) => {
          // Handle both number and array cases
          let coeffValue = 0;
          if (typeof coef === 'number') {
            coeffValue = coef;
          } else if (Array.isArray(coef) && coef.length > 0) {
            coeffValue = typeof coef[0] === 'number' ? coef[0] : 0;
          }
          return {
            variable: feature,
            coefficient: coeffValue,
          };
        })
        .filter(item => item.coefficient !== 0 || Object.keys(result.coefficients.features).length === 1)
        .sort((a, b) => Math.abs(b.coefficient) - Math.abs(a.coefficient));

      if (coefData.length > 0) {
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
    }

    return charts.length > 0 ? charts : undefined;
  }

  /**
   * Extract model context (target, features, model type) from chat history
   */
  private extractModelContextFromHistory(
    chatHistory: { role: string; content: string }[],
    availableColumns: string[]
  ): { target: string | null; features: string[]; modelType: string | null } {
    // Look at recent assistant messages for model results
    const recentMessages = chatHistory.slice(-10);
    
    let target: string | null = null;
    let features: string[] = [];
    let modelType: string | null = null;
    
    for (const msg of recentMessages.reverse()) {
      if (msg.role === 'assistant' && msg.content) {
        const content = msg.content;
        
        // Look for "Target Variable: X" pattern
        const targetMatch = content.match(/Target\s+Variable[:\s]+([^\n,]+)/i);
        if (targetMatch && !target) {
          const possibleTarget = targetMatch[1].trim();
          // Verify it's a valid column
          const matched = availableColumns.find(col => 
            col.toLowerCase() === possibleTarget.toLowerCase() ||
            col.toLowerCase().includes(possibleTarget.toLowerCase()) ||
            possibleTarget.toLowerCase().includes(col.toLowerCase())
          );
          if (matched) {
            target = matched;
          }
        }
        
        // Look for "Feature Variables: X, Y, Z" pattern
        const featuresMatch = content.match(/Feature\s+Variables?[:\s]+([^\n]+)/i);
        if (featuresMatch && features.length === 0) {
          const featureList = featuresMatch[1].split(/[,\s]+/).map(f => f.trim()).filter(f => f.length > 0);
          features = featureList
            .map(f => availableColumns.find(col => col.toLowerCase().includes(f.toLowerCase())))
            .filter((f): f is string => f !== undefined);
        }
        
        // Look for model type
        const modelMatch = content.match(/(linear|logistic|ridge|lasso|random\s*forest|decision\s*tree|gradient\s*boosting|elasticnet|svm|knn)\s*(regression|model|classification)?/i);
        if (modelMatch && !modelType) {
          const type = modelMatch[1].toLowerCase().replace(/\s+/g, '_');
          if (['linear', 'logistic', 'ridge', 'lasso', 'random_forest', 'decision_tree', 'gradient_boosting', 'elasticnet', 'svm', 'knn'].includes(type)) {
            modelType = type;
          }
        }
        
        // If we found target, we can stop
        if (target) break;
      }
    }
    
    return { target, features, modelType };
  }

  /**
   * Check if question is asking for advice/suggestions rather than performing an action
   */
  private isAdviceQuestion(question: string): boolean {
    const lower = question.toLowerCase();
    const advicePatterns = [
      /how\s+can\s+we\s+improve/i,
      /how\s+to\s+improve/i,
      /what\s+should\s+we\s+do/i,
      /what\s+would\s+help/i,
      /suggestions?\s+for/i,
      /recommendations?\s+for/i,
      /advice\s+on/i,
      /how\s+do\s+we\s+improve/i,
      /what\s+can\s+we\s+do\s+to/i,
      /how\s+should\s+we/i,
    ];
    
    return advicePatterns.some(pattern => pattern.test(lower));
  }

  /**
   * Handle advice questions with simple conversational responses (no charts, no new model training)
   * Uses the most recent model summary from chat history plus dataset summary for context.
   */
  private async handleAdviceQuestion(
    question: string,
    context: HandlerContext
  ): Promise<HandlerResponse> {
    const { getModelForTask } = await import('../models.js');
    const { openai } = await import('../../openai.js');

    // Find the most recent assistant message that looks like a model summary
    const recentMessages = [...context.chatHistory].reverse();
    let lastModelSummary: string | null = null;

    for (const msg of recentMessages) {
      if (msg.role === 'assistant' && msg.content) {
        if (
          msg.content.includes("I've successfully trained a") ||
          msg.content.includes('Model Summary:') ||
          msg.content.includes('Model Performance:')
        ) {
          lastModelSummary = msg.content;
          break;
        }
      }
    }

    const dataSummaryText = [
      `Rows: ${context.summary.rowCount}`,
      `Columns: ${context.summary.columns.map(c => c.name).join(', ')}`,
      context.summary.numericColumns?.length
        ? `Numeric columns: ${context.summary.numericColumns.join(', ')}`
        : null,
    ]
      .filter(Boolean)
      .join('\n');

    const modelContext = lastModelSummary
      ? `\n\nMOST RECENT MODEL SUMMARY (from previous answer):\n${lastModelSummary}`
      : '\n\nMOST RECENT MODEL SUMMARY: (none found in chat history)';

    const prompt = `You are a helpful data analyst assistant. The user is asking for advice or suggestions about improving their machine learning model.

USER QUESTION:
${question}

DATASET SUMMARY:
${dataSummaryText}
${modelContext}

TASK:
- Give practical, concrete suggestions for improving this specific model, based on the metrics and feature importance above.
- Reference the target variable and key features if they appear in the model summary.
- Mention ideas like: trying additional features, removing weak/noisy features, feature engineering, hyperparameter tuning, trying regularized models (ridge/lasso) or different algorithms, cross-validation, and collecting more data.
- Keep the answer short and clear (3–6 bullet points or 2–4 sentences).
- Do NOT train a new model or describe training steps. Focus only on advice using the existing results.`;

    try {
      const model = getModelForTask('generation');
      const response = await openai.chat.completions.create({
        model,
        messages: [
          {
            role: 'system',
            content:
              'You are a helpful data analyst assistant. Provide concise, actionable advice about improving models using the existing model summary and dataset, without training new models or generating charts.',
          },
          {
            role: 'user',
            content: prompt,
          },
        ],
        temperature: 0.7,
        max_tokens: 400,
      });

      const answer =
        response.choices[0].message.content?.trim() ||
        'Based on the current model performance, you could try testing additional relevant features, engineering new variables, and tuning the model hyperparameters (e.g., max depth, number of trees) to see if the fit improves.';

      return {
        answer,
        charts: [], // Explicitly no charts for advice questions
        insights: [],
      };
    } catch (error) {
      console.error('Error generating advice response:', error);
      return {
        answer:
          'Looking at the current model performance, you could try: (1) adding or removing features based on their importance, (2) doing feature engineering, and (3) tuning hyperparameters like depth, number of trees, or regularization strength to improve the fit.',
        charts: [],
        insights: [],
      };
    }
  }
}

