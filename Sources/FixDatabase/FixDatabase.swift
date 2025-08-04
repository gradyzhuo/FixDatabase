// The Swift Programming Language
// https://docs.swift.org/swift-book

import KurrentDB
import Foundation

@main
struct FixDatabase {
    static func main() async throws {
        let settings: ClientSettings = "kurrentdb://localhost:2113?tls=false"
        let client = KurrentDBClient(settings: settings)
        
        let settings2: ClientSettings = .localhost(port: 2114)
        let client2 = KurrentDBClient(settings: settings2)
        
        let responses = try await client.readAllStreams()

        let filteredResponses = responses.filter { response in
            do{
                let record = (try response.event).record
                return !record.eventType.hasPrefix("$") || !record.streamIdentifier.name.hasPrefix("$")
            }catch{
                print("error:", error)
                return false
            }
        }
        
        
        let recordEvents = try await filteredResponses.reduce(into: [RecordedEvent]()) { partialResult, response in
            guard let record = try? response.event.record else {
                return
            }
            
            partialResult.append(record)
        }
        
        
        
        let sortedEvents = recordEvents.sorted { (lhs, rhs) in
            guard let lhsJSON = try? JSONSerialization.jsonObject(with: lhs.data) as? [String: Any],
                  let rhsJSON = try? JSONSerialization.jsonObject(with: rhs.data) as? [String: Any],
                  let lhsTimeInterval = lhsJSON["occurred"] as? Double,
                  let rhsTimeInterval = rhsJSON["occurred"] as? Double
            else {
                return false
            }
            return lhsTimeInterval < rhsTimeInterval
        }
        
        for recordedEvent in sortedEvents {
            print(recordedEvent.streamIdentifier)
            let eventData = EventData(id: recordedEvent.id, like: recordedEvent)
            try await client2.appendStream(recordedEvent.streamIdentifier, events: [eventData])
        }

    }
}
