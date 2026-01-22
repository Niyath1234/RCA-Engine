import { create } from 'zustand';

interface ReasoningStep {
  id: string;
  type: 'thought' | 'action' | 'result' | 'error';
  content: string;
  timestamp: string;
  metadata?: any;
}

interface StoreState {
  reasoningSteps: ReasoningStep[];
  addReasoningStep: (step: ReasoningStep) => void;
  clearReasoning: () => void;
}

export const useStore = create<StoreState>((set) => ({
  reasoningSteps: [],
  addReasoningStep: (step: ReasoningStep) =>
    set((state) => ({
      reasoningSteps: [...state.reasoningSteps, step],
    })),
  clearReasoning: () => set({ reasoningSteps: [] }),
}));

