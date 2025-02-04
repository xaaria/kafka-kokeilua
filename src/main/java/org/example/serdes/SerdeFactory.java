package org.example.serdes;

import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.models.Coordinates;

/*
* Get Custom Serde(s) from "factory"
* */
public class SerdeFactory {

    public static Serde<Coordinates> getCoordinatesSerde() {
        return Serdes.serdeFrom(
            new CoordinatesSerializer(),
            new CoordinatesDeserializer()
        );
    }

}
