import { gql } from "@apollo/client/core";

export const GET_PIPELINE_STATUS = gql`
  query GetPipelineStatus {
    pipelineStatus
  }
`;

export const START_PIPELINE = gql`
  mutation StartPipeline {
    startPipeline
  }
`;

export const STOP_PIPELINE = gql`
  mutation StopPipeline {
    stopPipeline
  }
`;
