import { Request, Response } from "express";
import { 
  getUserChats, 
  getChatDocument, 
  deleteChatDocument,
  ChatDocument 
} from "../models/chat.model.js";

// Get all chats for a user
export const getUserChatHistory = async (req: Request, res: Response) => {
  try {
    const username = req.params.username || req.headers['x-user-email'] || req.body.username;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const chats = await getUserChats(username);
    
    // Return simplified chat list without full data
    const chatList = chats.map(chat => ({
      id: chat.id,
      fileName: chat.fileName,
      uploadedAt: chat.uploadedAt,
      createdAt: chat.createdAt,
      lastUpdatedAt: chat.lastUpdatedAt,
      messageCount: chat.messages.length,
      chartCount: chat.charts.length,
    }));

    res.json({ chats: chatList });
  } catch (error) {
    console.error('Get user chats error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to get user chats',
    });
  }
};

// Get specific chat details
export const getChatDetails = async (req: Request, res: Response) => {
  try {
    const { chatId } = req.params;
    const username = req.query.username as string || req.headers['x-user-email'] || req.body.username;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const chat = await getChatDocument(chatId, username);
    
    if (!chat) {
      return res.status(404).json({ error: 'Chat not found' });
    }

    res.json({ chat });
  } catch (error) {
    console.error('Get chat details error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to get chat details',
    });
  }
};

// Delete a chat
export const deleteChat = async (req: Request, res: Response) => {
  try {
    const { chatId } = req.params;
    const username = req.body.username || req.headers['x-user-email'];
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    await deleteChatDocument(chatId, username);
    
    res.json({ message: 'Chat deleted successfully' });
  } catch (error) {
    console.error('Delete chat error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to delete chat',
    });
  }
};

// Get chat statistics for a user
export const getChatStatistics = async (req: Request, res: Response) => {
  try {
    const username = req.params.username || req.headers['x-user-email'] || req.body.username;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const chats = await getUserChats(username);
    
    const stats = {
      totalChats: chats.length,
      totalMessages: chats.reduce((sum, chat) => sum + chat.messages.length, 0),
      totalCharts: chats.reduce((sum, chat) => sum + chat.charts.length, 0),
      totalFiles: new Set(chats.map(chat => chat.fileName)).size,
      lastActivity: chats.length > 0 ? Math.max(...chats.map(chat => chat.lastUpdatedAt)) : null,
    };

    res.json({ statistics: stats });
  } catch (error) {
    console.error('Get chat statistics error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to get chat statistics',
    });
  }
};

