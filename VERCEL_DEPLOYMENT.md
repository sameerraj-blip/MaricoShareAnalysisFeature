# Vercel Deployment Guide

## ✅ Setup Complete!

Your project is now configured for Vercel deployment. Here's what was set up:

### Files Created/Modified:

1. **`api/index.ts`** - Vercel serverless function entry point
2. **`vercel.json`** - Vercel configuration
3. **`server/index.ts`** - Updated to support both local and Vercel
4. **`package.json`** - Added `vercel-build` script

## 🚀 Deployment Steps

### Option 1: Deploy via Vercel Dashboard (Recommended)

1. **Go to Vercel**: https://vercel.com
2. **Sign in** with your GitHub account
3. **Click "Add New Project"**
4. **Import your repository**: 
   - Select `Chiragwork1998/MaricoInsightSafe`
   - Or connect your GitHub account and select the repo
5. **Configure Project**:
   - Framework Preset: **Other**
   - Root Directory: **./** (root)
   - Build Command: `npm run build` (already set in vercel.json)
   - Output Directory: `client/dist` (already set in vercel.json)
   - Install Command: `npm install` (already set)
6. **Add Environment Variables**:
   - Click "Environment Variables"
   - Add all your `.env` variables:
     - `OPENAI_API_KEY`
     - `AZURE_COSMOSDB_ENDPOINT`
     - `AZURE_COSMOSDB_KEY`
     - `AZURE_STORAGE_CONNECTION_STRING`
     - `AZURE_CLIENT_ID`
     - `AZURE_CLIENT_SECRET`
     - `AZURE_TENANT_ID`
     - Any other environment variables you use
7. **Click "Deploy"**

### Option 2: Deploy via Vercel CLI

```bash
# Install Vercel CLI
npm i -g vercel

# Login to Vercel
vercel login

# Deploy (from project root)
vercel

# Follow the prompts:
# - Link to existing project? No
# - Project name: marico-insight-safe (or your choice)
# - Directory: ./
# - Override settings? No

# For production deployment
vercel --prod
```

## 📋 Environment Variables Needed

Make sure to add these in Vercel Dashboard → Settings → Environment Variables:

### Required:
- `NODE_ENV=production`

### Azure OpenAI (REQUIRED - App won't work without these):
- `AZURE_OPENAI_API_KEY` - Your Azure OpenAI API key
- `AZURE_OPENAI_ENDPOINT` - Your Azure OpenAI endpoint (e.g., `https://your-resource.openai.azure.com`)
- `AZURE_OPENAI_DEPLOYMENT_NAME` - Your Azure OpenAI deployment name (e.g., `gpt-4o`)
- `AZURE_OPENAI_API_VERSION` - API version (optional, defaults to `2024-02-15-preview`)

### Azure AD (Required for Authentication):
- `VITE_AZURE_CLIENT_ID` - Your Azure AD Client ID (`5e4faaa4-8f8b-4766-a2d5-d382004beea2`)
- `VITE_AZURE_TENANT_ID` - Your Azure AD Tenant ID
- `VITE_AZURE_REDIRECT_URI` - `https://marico-insight-safe.vercel.app` (optional, code uses window.location.origin)
- `VITE_AZURE_POST_LOGOUT_REDIRECT_URI` - `https://marico-insight-safe.vercel.app` (optional)

### Optional (if using):
- `AZURE_COSMOSDB_ENDPOINT`
- `AZURE_COSMOSDB_KEY`
- `AZURE_STORAGE_CONNECTION_STRING`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`

## ⚠️ IMPORTANT: Azure AD Configuration

**You MUST add your Vercel URL to Azure AD redirect URIs!**

1. Go to [Azure Portal](https://portal.azure.com) → Azure AD → App registrations
2. Find app: `5e4faaa4-8f8b-4766-a2d5-d382004beea2`
3. Go to **Authentication** → **Single-page application**
4. Add redirect URI: `https://marico-insight-safe.vercel.app`
5. Save

See `VERCEL_AZURE_AD_FIX.md` for detailed instructions.

## 🔧 How It Works

1. **Frontend**: Built from `client/` folder → Output to `client/dist`
2. **Backend**: Express app wrapped as serverless function in `api/index.ts`
3. **Routing**:
   - `/api/*` → Serverless function (Express app)
   - `/*` → Static files from `client/dist`

## ⚠️ Important Notes

### Vercel Limitations:
- **Function timeout**: 60 seconds (configured in vercel.json)
- **Memory**: 3008 MB (configured)
- **File uploads**: Limited to 4.5MB per request (Vercel limit)
- **WebSockets**: Not supported (if you use WebSockets, consider Railway/Render)

### If You Have Issues:

1. **Check build logs** in Vercel dashboard
2. **Check function logs** for runtime errors
3. **Verify environment variables** are set correctly
4. **Test locally first**: `npm run build` should work

## 🧪 Testing Deployment

After deployment, test these endpoints:

- `https://your-app.vercel.app/api/health` - Should return `{status: 'OK'}`
- `https://your-app.vercel.app/` - Should show your React app
- `https://your-app.vercel.app/api/chat` - Test chat endpoint

## 📝 Next Steps

1. Deploy to Vercel using steps above
2. Test all functionality
3. Set up custom domain (optional)
4. Configure production environment variables
5. Monitor function logs for any issues

## 🔄 Updating Deployment

After making changes:

```bash
# Commit and push
git add .
git commit -m "Your changes"
git push

# Vercel will auto-deploy if connected to GitHub
# Or manually deploy:
vercel --prod
```

## 🆘 Troubleshooting

### Build Fails:
- Check `client/package.json` has correct build script
- Ensure all dependencies are in `package.json`
- Check Vercel build logs

### API Routes Not Working:
- Verify `api/index.ts` exists
- Check function logs in Vercel dashboard
- Ensure environment variables are set

### Frontend Not Loading:
- Check `outputDirectory` in vercel.json matches build output
- Verify `client/dist` folder is created after build
- Check build logs for errors

---

**Ready to deploy!** 🚀

