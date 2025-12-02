/**
 * Agent System Entry Point
 * Initializes and exports the orchestrator with all handlers registered
 */

import { getOrchestrator } from './orchestrator.js';
import { ConversationalHandler } from './handlers/conversationalHandler.js';
import { StatisticalHandler } from './handlers/statisticalHandler.js';
import { ComparisonHandler } from './handlers/comparisonHandler.js';
import { CorrelationHandler } from './handlers/correlationHandler.js';
import { MLModelHandler } from './handlers/mlModelHandler.js';
import { GeneralHandler } from './handlers/generalHandler.js';

/**
 * Initialize the agent system with all handlers
 */
export function initializeAgents() {
  const orchestrator = getOrchestrator();

  // Register handlers in priority order
  // More specific handlers should be registered first
  orchestrator.registerHandler(new ConversationalHandler());
  orchestrator.registerHandler(new MLModelHandler()); // ML model handler before other analysis handlers
  orchestrator.registerHandler(new StatisticalHandler()); // Statistical before correlation (for "which month" queries)
  orchestrator.registerHandler(new ComparisonHandler()); // Comparison before correlation (for "best competitor" queries)
  orchestrator.registerHandler(new CorrelationHandler());
  orchestrator.registerHandler(new GeneralHandler()); // General handler last (catch-all)

  console.log('✅ Agent system initialized with handlers');
  return orchestrator;
}

/**
 * Get initialized orchestrator
 */
let isInitialized = false;

export function getInitializedOrchestrator() {
  // Initialize if not already done
  if (!isInitialized) {
    initializeAgents();
    isInitialized = true;
  }
  return getOrchestrator();
}

export { getOrchestrator } from './orchestrator.js';
export { classifyIntent } from './intentClassifier.js';
export type { AnalysisIntent } from './intentClassifier.js';
export { resolveContextReferences } from './contextResolver.js';
export { retrieveContext } from './contextRetriever.js';

