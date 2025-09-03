// API Configuration for ARGO Backend
export const API_CONFIG = {
  // Default to localhost development setup
  BASE_URL: import.meta.env.VITE_API_URL || 'http://localhost:8000',
  
  // API endpoints based on your FastAPI backend
  ENDPOINTS: {
    QUERY: '/query',
    FLOATS: '/floats',
    FLOAT_DETAILS: '/float',
    UPLOAD_NETCDF: '/upload_netcdf',
    EXPORT_NETCDF: '/export_netcdf',
    EXPORT_CSV: '/export_csv',
    EXPORT_PARQUET: '/export_parquet',
    HEALTH: '/health'
  }
};

// Helper function to build full API URL
export const buildApiUrl = (endpoint: string, params?: Record<string, string>) => {
  const url = new URL(endpoint, API_CONFIG.BASE_URL);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      url.searchParams.append(key, value);
    });
  }
  return url.toString();
};

// Helper function to check if we're in development
export const isDevelopment = () => {
  return import.meta.env.DEV;
};