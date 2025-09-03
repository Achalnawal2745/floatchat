import React, { useState, useEffect } from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { CheckCircle, XCircle, Loader2, RefreshCw } from "lucide-react";
import { buildApiUrl, API_CONFIG } from "@/config/api";

interface ConnectionStatusProps {
  onStatusChange?: (connected: boolean) => void;
}

export const ConnectionStatus: React.FC<ConnectionStatusProps> = ({ onStatusChange }) => {
  const [isConnected, setIsConnected] = useState<boolean | null>(null);
  const [isChecking, setIsChecking] = useState(false);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);

  const checkConnection = async () => {
    setIsChecking(true);
    try {
      const response = await fetch(buildApiUrl(API_CONFIG.ENDPOINTS.HEALTH), {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      const connected = response.ok;
      setIsConnected(connected);
      setLastChecked(new Date());
      onStatusChange?.(connected);
      
      if (connected) {
        const data = await response.json();
        console.log('Backend health check:', data);
      }
    } catch (error) {
      console.error('Connection check failed:', error);
      setIsConnected(false);
      setLastChecked(new Date());
      onStatusChange?.(false);
    } finally {
      setIsChecking(false);
    }
  };

  useEffect(() => {
    checkConnection();
    // Check connection every 30 seconds
    const interval = setInterval(checkConnection, 30000);
    return () => clearInterval(interval);
  }, []);

  const getStatusColor = () => {
    if (isConnected === null) return "bg-muted";
    return isConnected ? "bg-success" : "bg-destructive";
  };

  const getStatusIcon = () => {
    if (isChecking) return <Loader2 className="h-4 w-4 animate-spin" />;
    if (isConnected === null) return <Loader2 className="h-4 w-4" />;
    return isConnected ? 
      <CheckCircle className="h-4 w-4" /> : 
      <XCircle className="h-4 w-4" />;
  };

  const getStatusText = () => {
    if (isConnected === null) return "Checking...";
    return isConnected ? "Connected" : "Disconnected";
  };

  return (
    <Card className="border-l-4 border-l-primary/20">
      <CardContent className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <Badge 
              className={`${getStatusColor()}/10 text-foreground border-${getStatusColor().replace('bg-', '')}/20`}
            >
              {getStatusIcon()}
              <span className="ml-2">{getStatusText()}</span>
            </Badge>
            <div className="text-sm text-muted-foreground">
              <div>Backend: {API_CONFIG.BASE_URL}</div>
              {lastChecked && (
                <div className="text-xs">
                  Last checked: {lastChecked.toLocaleTimeString()}
                </div>
              )}
            </div>
          </div>
          
          <Button 
            variant="outline" 
            size="sm" 
            onClick={checkConnection}
            disabled={isChecking}
          >
            <RefreshCw className={`h-4 w-4 ${isChecking ? 'animate-spin' : ''}`} />
          </Button>
        </div>
        
        {!isConnected && isConnected !== null && (
          <div className="mt-3 p-3 bg-destructive/5 rounded-lg border border-destructive/20">
            <h4 className="text-sm font-medium text-destructive mb-2">Backend Connection Failed</h4>
            <p className="text-xs text-muted-foreground mb-2">
              Make sure your FastAPI backend is running at: <code className="bg-muted px-1 rounded">{API_CONFIG.BASE_URL}</code>
            </p>
            <div className="text-xs text-muted-foreground space-y-1">
              <div>1. Navigate to your backend directory</div>
              <div>2. Install dependencies: <code className="bg-muted px-1 rounded">pip install -r requirements.txt</code></div>
              <div>3. Start the server: <code className="bg-muted px-1 rounded">uvicorn main:app --reload --host 0.0.0.0 --port 8000</code></div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
};