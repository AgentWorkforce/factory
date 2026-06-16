import { createFactoryNodeDefinition, readFactoryNodeConfigSync } from './factory-node'

export default createFactoryNodeDefinition({
  config: readFactoryNodeConfigSync(),
})
