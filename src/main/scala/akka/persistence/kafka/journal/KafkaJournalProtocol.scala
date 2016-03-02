package akka.persistence.kafka.journal

import akka.actor._

object KafkaJournalProtocol {
  /**
   * Request to read the highest stored sequence number of a given persistent actor.
   *
   * @param fromSequenceNr optional hint where to start searching for the maximum sequence number.
   * @param persistenceId requesting persistent actor id.
   * @param persistentActor requesting persistent actor.
   */
  case class ReadHighestSequenceNr(fromSequenceNr: Long = 1L, persistenceId: String, persistentActor: ActorRef)

  /**
   * Reply message to a successful [[ReadHighestSequenceNr]] request.
   *
   * @param highestSequenceNr read highest sequence number.
   */
  case class ReadHighestSequenceNrSuccess(highestSequenceNr: Long)

  /**
   * Reply message to a failed [[ReadHighestSequenceNr]] request.
   *
   * @param cause failure cause.
   */
  case class ReadHighestSequenceNrFailure(cause: Throwable)
}
