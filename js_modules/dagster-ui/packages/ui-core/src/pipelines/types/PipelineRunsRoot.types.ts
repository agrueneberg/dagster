// Generated GraphQL types, do not edit manually.

import * as Types from '../../graphql/types';

export type PipelineRunsRootQueryVariables = Types.Exact<{
  limit?: Types.InputMaybe<Types.Scalars['Int']['input']>;
  cursor?: Types.InputMaybe<Types.Scalars['String']['input']>;
  filter: Types.RunsFilter;
}>;

export type PipelineRunsRootQuery = {
  __typename: 'Query';
  pipelineRunsOrError:
    | {__typename: 'InvalidPipelineRunsFilterError'; message: string}
    | {
        __typename: 'PythonError';
        message: string;
        stack: Array<string>;
        errorChain: Array<{
          __typename: 'ErrorChainLink';
          isExplicitLink: boolean;
          error: {__typename: 'PythonError'; message: string; stack: Array<string>};
        }>;
      }
    | {
        __typename: 'Runs';
        results: Array<{
          __typename: 'Run';
          id: string;
          status: Types.RunStatus;
          stepKeysToExecute: Array<string> | null;
          canTerminate: boolean;
          hasReExecutePermission: boolean;
          hasTerminatePermission: boolean;
          hasDeletePermission: boolean;
          hasRunMetricsEnabled: boolean;
          mode: string;
          rootRunId: string | null;
          parentRunId: string | null;
          pipelineSnapshotId: string | null;
          pipelineName: string;
          solidSelection: Array<string> | null;
          creationTime: number;
          startTime: number | null;
          endTime: number | null;
          updateTime: number | null;
          repositoryOrigin: {
            __typename: 'RepositoryOrigin';
            id: string;
            repositoryName: string;
            repositoryLocationName: string;
          } | null;
          assetSelection: Array<{__typename: 'AssetKey'; path: Array<string>}> | null;
          assetCheckSelection: Array<{
            __typename: 'AssetCheckhandle';
            name: string;
            assetKey: {__typename: 'AssetKey'; path: Array<string>};
          }> | null;
          tags: Array<{__typename: 'PipelineTag'; key: string; value: string}>;
          launchedBy: {
            __typename: 'LaunchedBy';
            kind: string;
            tag: {__typename: 'PipelineTag'; key: string; value: string};
          };
        }>;
      };
};

export const PipelineRunsRootQueryVersion = 'f91e8893973039d57fb3a7ef15e4722182264126c7d25439620a08f063875dd0';
