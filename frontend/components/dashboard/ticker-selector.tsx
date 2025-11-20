'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface TickerSelectorProps {
  tickers: string[];
}

export function TickerSelector({ tickers }: TickerSelectorProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const currentTicker = searchParams.get('ticker') || 'ALL';

  const handleValueChange = (value: string) => {
    const params = new URLSearchParams(searchParams.toString());
    if (value === 'ALL') {
      params.delete('ticker');
    } else {
      params.set('ticker', value);
    }
    router.push(`?${params.toString()}`);
  };

  return (
    <div className="w-[200px]">
      <Select value={currentTicker} onValueChange={handleValueChange}>
        <SelectTrigger>
          <SelectValue placeholder="Select Ticker" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="ALL">All Stocks</SelectItem>
          {tickers.map((ticker) => (
            <SelectItem key={ticker} value={ticker}>
              {ticker}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}