/**
 * SSE Connection Limiter
 * Prevents too many concurrent SSE connections per user
 */

const MAX_CONNECTIONS_PER_USER = 3;
const userConnections = new Map<string, number>();

export function checkSSELimit(userEmail: string): boolean {
  const count = userConnections.get(userEmail) || 0;
  if (count >= MAX_CONNECTIONS_PER_USER) {
    return false;
  }
  userConnections.set(userEmail, count + 1);
  return true;
}

export function releaseSSEConnection(userEmail: string): void {
  const count = userConnections.get(userEmail) || 0;
  if (count > 0) {
    userConnections.set(userEmail, count - 1);
  }
}

// Cleanup stale entries every 5 minutes
setInterval(() => {
  for (const [email, count] of userConnections.entries()) {
    if (count === 0) {
      userConnections.delete(email);
    }
  }
}, 5 * 60 * 1000);

