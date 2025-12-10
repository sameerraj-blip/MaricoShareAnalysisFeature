/**
 * Data Ops Controller
 * Thin controller layer for data operations endpoints - delegates to services
 */
import { Request, Response } from "express";
import { processDataOperation } from "../services/dataOps/dataOps.service.js";
import { processStreamDataOperation } from "../services/dataOps/dataOpsStream.service.js";
import { requireUsername } from "../utils/auth.helper.js";
import { sendError, sendValidationError, sendNotFound } from "../utils/responseFormatter.js";
import { getChatBySessionIdEfficient } from "../models/chat.model.js";
import { loadLatestData } from "../utils/dataLoader.js";
import * as XLSX from 'xlsx';

/**
 * Non-streaming Data Ops chat endpoint
 */
export const dataOpsChatWithAI = async (req: Request, res: Response) => {
  try {
    console.log('📨 dataOpsChatWithAI() called');
    const { sessionId, message, chatHistory, dataOpsMode } = req.body;
    const username = requireUsername(req);

    // Validate required fields
    if (!sessionId || !message) {
      return sendValidationError(res, 'Missing required fields');
    }

    // Process data operation
    const result = await processDataOperation({
      sessionId,
      message,
      chatHistory,
      dataOpsMode,
      username,
    });

    res.json(result);
  } catch (error) {
    console.error('Data Ops chat error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to process data operation';
    sendError(res, errorMessage);
  }
};

/**
 * Streaming Data Ops chat endpoint using Server-Sent Events (SSE)
 */
export const dataOpsChatWithAIStream = async (req: Request, res: Response) => {
  try {
    console.log('📨 dataOpsChatWithAIStream() called');
    const { sessionId, message, chatHistory, dataOpsMode } = req.body;
    const username = requireUsername(req);

    // Validate required fields
    if (!sessionId || !message) {
      return;
    }
    
    // Process streaming data operation
    await processStreamDataOperation({
      sessionId,
      message,
      chatHistory,
      dataOpsMode,
      username,
      res,
    });
  } catch (error) {
    console.error('Data Ops stream error:', error);
    // Error handling is done in the service
  }
};

/**
 * Download modified dataset as CSV or Excel
 */
export const downloadModifiedDataset = async (req: Request, res: Response) => {
  try {
    console.log('📥 downloadModifiedDataset() called');
    const { sessionId } = req.params;
    const format = (req.query.format as string) || 'csv'; // csv or xlsx
    const username = requireUsername(req);

    if (!sessionId) {
      return sendValidationError(res, 'Session ID is required');
    }

    // Get chat document
    const chatDocument = await getChatBySessionIdEfficient(sessionId);
    if (!chatDocument) {
      return sendNotFound(res, 'Session not found');
    }

    // Verify user has access to this session
    if (chatDocument.username.toLowerCase() !== username.toLowerCase()) {
      return res.status(403).json({ error: 'Access denied' });
    }

    // Load the latest modified data
    const data = await loadLatestData(chatDocument);
    
    if (!data || data.length === 0) {
      return sendError(res, 'No data available to download');
    }

    // Get filename from original file or generate one
    const originalFileName = chatDocument.fileName || 'dataset';
    const baseFileName = originalFileName.replace(/\.[^/.]+$/, ''); // Remove extension
    const timestamp = new Date().toISOString().split('T')[0]; // YYYY-MM-DD

    if (format === 'xlsx') {
      // Convert to Excel
      const worksheet = XLSX.utils.json_to_sheet(data);
      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');
      
      // Generate buffer
      const excelBuffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
      
      // Set headers
      res.setHeader('Content-Disposition', `attachment; filename="${baseFileName}_modified_${timestamp}.xlsx"`);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Length', excelBuffer.length);
      
      res.send(excelBuffer);
    } else {
      // Convert to CSV
      if (data.length === 0) {
        return sendError(res, 'No data to export');
      }

      // Get all column names
      const columns = Object.keys(data[0] || {});
      
      // Create CSV header
      const csvHeader = columns.map(col => `"${String(col).replace(/"/g, '""')}"`).join(',');
      
      // Create CSV rows
      const csvRows = data.map(row => {
        return columns.map(col => {
          const value = row[col];
          if (value === null || value === undefined) {
            return '';
          }
          // Escape quotes and wrap in quotes if contains comma, newline, or quote
          const stringValue = String(value);
          if (stringValue.includes(',') || stringValue.includes('\n') || stringValue.includes('"')) {
            return `"${stringValue.replace(/"/g, '""')}"`;
          }
          return stringValue;
        }).join(',');
      });
      
      // Combine header and rows
      const csvContent = [csvHeader, ...csvRows].join('\n');
      const csvBuffer = Buffer.from(csvContent, 'utf-8');
      
      // Set headers
      res.setHeader('Content-Disposition', `attachment; filename="${baseFileName}_modified_${timestamp}.csv"`);
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Length', csvBuffer.length);
      
      res.send(csvBuffer);
    }
  } catch (error) {
    console.error('Download modified dataset error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to download dataset';
    sendError(res, errorMessage);
  }
};
