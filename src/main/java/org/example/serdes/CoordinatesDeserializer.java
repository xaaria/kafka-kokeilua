package org.example.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.example.models.Coordinates;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class CoordinatesDeserializer implements Deserializer<Coordinates> {

    @Override
    public Coordinates deserialize(String s, byte[] bytes) {

        ObjectInputStream in = null;

        try {

            in = new ObjectInputStream(new ByteArrayInputStream(bytes));
            Coordinates coords = (Coordinates) in.readObject();
            in.close();
            return coords;

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
