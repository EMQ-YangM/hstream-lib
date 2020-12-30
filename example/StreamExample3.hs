{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

import Data.Aeson
import qualified Data.Binary as B
import Data.Maybe
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
import HStream.Encoding
import HStream.Processor
import HStream.Store
import qualified HStream.Stream as HS
import qualified HStream.Stream.GroupedStream as HG
import HStream.Stream.SessionWindows
import qualified HStream.Table as HT
import HStream.Topic
import HStream.Util
import RIO
import System.Random
import qualified Prelude as P
import HStream.Stream.SessionWindowedStream as HSW

data R = R
  { temperature :: Int,
    humidity :: Int
  }
  deriving (Generic, Show, Typeable)

instance ToJSON R

instance FromJSON R

main :: IO ()
main = do
  let textSerde =
        Serde
          { serializer = Serializer TLE.encodeUtf8,
            deserializer = Deserializer TLE.decodeUtf8
          } ::
          Serde TL.Text

  let rSerde =
        Serde
          { serializer = Serializer encode,
            deserializer = Deserializer $ fromJust . decode
          } ::
          Serde R

  let intSerde =
        Serde
          { serializer = Serializer B.encode,
            deserializer = Deserializer $ B.decode
          } ::
          Serde Int

  let streamSourceConfig =
        HS.StreamSourceConfig
          { sscTopicName = "demo-source",
            sscKeySerde = textSerde,
            sscValueSerde = rSerde
          }

  let streamSinkConfig =
        HS.StreamSinkConfig
          { sicTopicName = "demo-sink",
            sicKeySerde = sessionWindowKeySerde textSerde,
            sicValueSerde = intSerde
          }

  aggStore <- mkInMemoryStateSessionStore
  let materialized =
        HS.Materialized
          { mKeySerde = textSerde,
            mValueSerde = intSerde,
            mStateStore = aggStore
          }
  streamBuilder <-
    HS.mkStreamBuilder "demo"
      >>= HS.stream streamSourceConfig
      >>= HS.filter filterR
      >>= HS.groupBy (fromJust . recordKey)
      >>= HG.sessionWindowedBy (mkSessionWindows 10000)
      >>= HSW.count materialized
      >>= HT.toStream
      >>= HS.to streamSinkConfig

  mockStore <- mkMockTopicStore
  mp <- mkMockTopicProducer mockStore
  mc' <- mkMockTopicConsumer mockStore

  _ <- async $
    forever $ do
      threadDelay 1000000
      MockMessage {..} <- mkMockData
      send
        mp
        RawProducerRecord
          { rprTopic = "demo-source",
            rprKey = mmKey,
            rprValue = mmValue,
            rprTimestamp = mmTimestamp
          }

  mc <- subscribe mc' ["demo-sink"]
  _ <- async $
    forever $ do
      records <- pollRecords mc 1000000
      forM_ records $ \RawConsumerRecord {..} -> do
        let k = runDeser (sessionWindowKeyDeserializer (deserializer textSerde)) (fromJust rcrKey)
        P.putStrLn $
          ">>> count: key: "
            ++ show k
            ++ " , value: "
            ++ show (B.decode rcrValue :: Int)

  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    let taskConfig =
          TaskConfig
            { tcMessageStoreType = Mock mockStore,
              tcLogFunc = lf
            }
    runTask taskConfig (HS.build streamBuilder)

filterR :: Record TL.Text R -> Bool
filterR Record {..} =
  temperature recordValue >= 0
    && humidity recordValue >= 0

mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 1)) :: IO Int
  t <- getStdRandom (randomR (0, 100))
  h <- getStdRandom (randomR (0, 100))
  let r = R {temperature = t, humidity = h}
  P.putStrLn $ "gen data: " ++ " key: " ++ show k ++ ", value: " ++ show r
  ts <- getCurrentTimestamp
  return
    MockMessage
      { mmTimestamp = ts,
        mmKey = Just $ TLE.encodeUtf8 $ TL.pack $ show k,
        mmValue = encode $ R {temperature = t, humidity = h}
      }
