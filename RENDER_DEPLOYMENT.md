# Render Deployment Configuration for Large Files

## File Upload Size Limits

The application now supports files up to **1GB** in size. However, Render may have additional limits:

### Current Configuration:
- **Multer limit**: 1GB (1024MB)
- **Express body parser**: 1GB
- **Large file processing**: Automatically enabled for files >= 50MB

### Render-Specific Considerations:

1. **Rebuild Required**: After updating file size limits, you must rebuild and redeploy:
   ```bash
   npm run build
   ```

2. **Render Service Limits**: 
   - Free tier: May have lower limits
   - Paid tiers: Should support larger files
   - Check Render dashboard for service-specific limits

3. **Request Timeout**: Large file uploads may take time. Ensure Render service timeout is sufficient:
   - Default: 30 seconds (may need to increase)
   - For 88MB+ files, consider 60-120 seconds

4. **Memory Limits**: 
   - Ensure Render service has enough memory (at least 2GB recommended for large files)
   - The app uses streaming processing to minimize memory usage

### Troubleshooting:

If you still get "File too large" errors after rebuilding:

1. **Check Render Service Settings**:
   - Go to Render Dashboard → Your Service → Settings
   - Check "Request Timeout" (increase if needed)
   - Check "Memory" allocation

2. **Verify Build**:
   - Ensure `npm run build` completed successfully
   - Check that `dist/index.js` has the updated limits (1GB, not 10MB)

3. **Check Logs**:
   - Look for multer error details in Render logs
   - Verify the actual file size being received

4. **Alternative**: If Render has hard limits, consider:
   - Using Azure Blob Storage direct upload (client → blob → server)
   - Chunked upload implementation
   - Using a different hosting provider for file uploads

