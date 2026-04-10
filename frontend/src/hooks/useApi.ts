import { useState, useEffect, useCallback, useRef } from 'react';

export interface UseApiState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useApi<T>(
  fetcher: (signal?: AbortSignal) => Promise<T>,
  deps: unknown[] = []
): UseApiState<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);

  // Track whether this trigger is a deps change vs a refetch/poll
  const prevDepsRef = useRef<unknown[]>(deps);
  const isRefetchRef = useRef(false);

  const refetch = useCallback(() => {
    isRefetchRef.current = true;
    setRevision(r => r + 1);
  }, []);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    // Deps actually changed (not just a refetch) → clear stale data
    const depsChanged = deps.some((d, i) => d !== prevDepsRef.current[i])
      || deps.length !== prevDepsRef.current.length;
    prevDepsRef.current = deps;

    if (depsChanged) {
      // Route/param change: wipe old data so we don't show stale content
      setData(null);
    }
    // refetch/poll: keep existing data visible while loading

    isRefetchRef.current = false;
    setLoading(true);
    setError(null);

    fetcher(controller.signal)
      .then(result => {
        if (!cancelled) {
          setData(result);
          setLoading(false);
        }
      })
      .catch(err => {
        if (err instanceof DOMException && err.name === 'AbortError') {
          return;
        }

        if (!cancelled) {
          // Only clear data on deps change (route navigation);
          // on refetch/poll failure, keep showing previous data
          if (depsChanged) setData(null);
          setError(err instanceof Error ? err.message : String(err));
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [revision, ...deps]);

  return { data, loading, error, refetch };
}
