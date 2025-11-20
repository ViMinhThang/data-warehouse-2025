  if (!result.success) {
    throw new Error(result.message || 'Failed to fetch stock ranking');
  }
  
  return result.data;
}

export async function fetchTickers(): Promise<string[]> {
  const url = `${API_BASE_URL}/api/tickers`;
  
  const response = await fetch(url, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`Failed to fetch tickers: ${response.statusText}`);
  }
  
  const result: ApiResponse<string[]> = await response.json();
  if (!result.success) {
    throw new Error(result.message || 'Failed to fetch tickers');
  }
  
  return result.data;
}
