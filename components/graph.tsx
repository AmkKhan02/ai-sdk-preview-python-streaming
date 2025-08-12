"use client";

import React from 'react';

interface GraphProps {
  graphData: {
    image?: string;
    error?: string;
  };
}

export const Graph: React.FC<GraphProps> = ({ graphData }) => {
  if (graphData.error) {
    return <div className="text-red-500">Error generating graph: {graphData.error}</div>;
  }

  if (!graphData.image) {
    return <div className="skeleton w-full h-64" />;
  }

  return (
    <div className="w-full">
      <img src={`data:image/png;base64,${graphData.image}`} alt="Generated Graph" />
    </div>
  );
};
