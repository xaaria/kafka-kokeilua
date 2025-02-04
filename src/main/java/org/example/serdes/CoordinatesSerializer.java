package org.example.serdes;

import org.apache.kafka.common.serialization.Serializer;
import org.example.models.Coordinates;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class CoordinatesSerializer
        implements Serializer<Coordinates> {

    @Override
    public byte[] serialize(String s, Coordinates coordinates) throws RuntimeException {
        try (
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos)
        ) {
            oos.writeObject(coordinates);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
