# MVC Refactoring Guide

This document outlines the new MVC architecture structure and how to complete the refactoring.

## New Structure

```
server/
├── models/              # Database models (M in MVC)
│   ├── database.config.ts    # Database connection & initialization
│   ├── chat.model.ts         # Chat/Session database operations
│   ├── dashboard.model.ts    # Dashboard database operations
│   ├── sharedAnalysis.model.ts # Shared analysis database operations
│   └── index.ts              # Central export
│
├── services/            # Business logic layer
│   ├── chat/
│   │   ├── chat.service.ts          # Chat business logic
│   │   ├── chatResponse.service.ts  # Response processing
│   │   └── chatStream.service.ts    # Streaming logic
│   ├── dataOps/
│   │   ├── dataOpsIntent.service.ts      # Intent parsing
│   │   ├── dataOpsExecution.service.ts   # Operation execution
│   │   └── dataOpsClarification.service.ts # Clarification flow
│   └── ...
│
├── controllers/         # Request handlers (C in MVC) - THIN LAYER
│   ├── chatController.ts
│   ├── dashboardController.ts
│   └── ...
│
├── utils/              # Utility functions
│   ├── sse.helper.ts        # SSE utilities
│   ├── responseFormatter.ts # Response formatting
│   ├── auth.helper.ts       # Auth utilities
│   └── index.ts
│
├── routes/             # Route definitions
├── middleware/         # Express middleware
└── lib/                # Legacy libraries (to be gradually moved to services)
```

## Completed ✅

1. ✅ Created `models/` directory with:
   - `database.config.ts` - Database connection
   - `chat.model.ts` - All chat-related database operations
   - `dashboard.model.ts` - All dashboard-related database operations
   - `sharedAnalysis.model.ts` - All shared analysis operations
   - `index.ts` - Central exports

2. ✅ Created `utils/` directory with:
   - `sse.helper.ts` - SSE utilities
   - `responseFormatter.ts` - Response formatting
   - `auth.helper.ts` - Auth utilities

3. ✅ Updated `server/index.ts` to use new models

## Next Steps

### 1. Create Service Layer

Create service files to extract business logic from controllers:

**`services/chat/chat.service.ts`**
- Extract chat processing logic from `chatController.ts`
- Handle message processing, chart enrichment, insights generation
- Should call models for data access

**`services/chat/chatResponse.service.ts`**
- Handle response validation and formatting
- Chart enrichment logic
- Insights derivation

**`services/chat/chatStream.service.ts`**
- Handle streaming chat logic
- SSE event management

**`services/dataOps/dataOpsIntent.service.ts`**
- Extract intent parsing from `dataOpsOrchestrator.ts`
- Intent classification logic

**`services/dataOps/dataOpsExecution.service.ts`**
- Extract operation execution from `dataOpsOrchestrator.ts`
- Data transformation logic

### 2. Refactor Controllers

Make controllers thin - they should only:
- Extract request data
- Call services
- Format and send responses
- Handle errors

Example:
```typescript
export const chatWithAI = async (req: Request, res: Response) => {
  try {
    const username = requireUsername(req);
    const { sessionId, message, chatHistory, targetTimestamp } = req.body;
    
    // Validate input
    if (!sessionId || !message) {
      return sendValidationError(res, 'Missing required fields');
    }
    
    // Call service
    const result = await chatService.processChatMessage({
      sessionId,
      message,
      chatHistory,
      targetTimestamp,
      username
    });
    
    // Send response
    sendSuccess(res, result);
  } catch (error) {
    sendError(res, error);
  }
};
```

### 3. Update Imports

Update all files to use new structure:

**Old:**
```typescript
import { getChatBySessionIdForUser } from "../lib/cosmosDB.js";
```

**New:**
```typescript
import { getChatBySessionIdForUser } from "../models/chat.model.js";
```

### 4. Break Down Large Files

**`dataOpsOrchestrator.ts` (1335 lines)**
- Split into:
  - `services/dataOps/dataOpsIntent.service.ts` - Intent parsing
  - `services/dataOps/dataOpsExecution.service.ts` - Execution
  - `services/dataOps/dataOpsClarification.service.ts` - Clarification

**`chatController.ts` (574 lines)**
- Split into:
  - `services/chat/chat.service.ts` - Main logic
  - `services/chat/chatResponse.service.ts` - Response handling
  - `services/chat/chatStream.service.ts` - Streaming

### 5. Migration Strategy

1. **Phase 1**: Update imports to use new models (keep old cosmosDB.ts temporarily)
2. **Phase 2**: Create service layer and move business logic
3. **Phase 3**: Refactor controllers to be thin
4. **Phase 4**: Remove old `lib/cosmosDB.ts` file
5. **Phase 5**: Test all endpoints

## Benefits

1. **Separation of Concerns**: Clear boundaries between data, business logic, and presentation
2. **Testability**: Services can be tested independently
3. **Maintainability**: Smaller, focused files are easier to understand
4. **Reusability**: Services can be reused across different controllers
5. **Scalability**: Easy to add new features without touching existing code

## Naming Conventions

- **Models**: `*.model.ts` - Database operations only
- **Services**: `*.service.ts` - Business logic
- **Controllers**: `*Controller.ts` - Request/response handling
- **Utils**: `*.helper.ts` or `*.util.ts` - Pure utility functions

## Example: Complete Flow

```
Request → Route → Controller → Service → Model → Database
                ↓
            Response ← Formatter ← Service ← Model
```

**Controller** (thin):
```typescript
export const createDashboard = async (req: Request, res: Response) => {
  try {
    const username = requireUsername(req);
    const { name, charts } = req.body;
    const dashboard = await dashboardService.createDashboard(username, name, charts);
    sendSuccess(res, dashboard, 201);
  } catch (error) {
    sendError(res, error);
  }
};
```

**Service** (business logic):
```typescript
export const createDashboard = async (username: string, name: string, charts: ChartSpec[]) => {
  // Business logic: validate, transform, etc.
  if (!name || name.trim().length === 0) {
    throw new Error('Dashboard name is required');
  }
  
  // Call model for data access
  return await createDashboardModel(username, name.trim(), charts);
};
```

**Model** (data access):
```typescript
export const createDashboardModel = async (username: string, name: string, charts: ChartSpec[]) => {
  // Direct database operations
  const container = await waitForDashboardsContainer();
  // ... database code
};
```

