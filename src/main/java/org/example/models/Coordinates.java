package org.example.models;

import java.io.Serializable;
import java.util.Objects;

public record Coordinates(double latitude, double longitude)
        implements Serializable {

    @Override
    public String toString() {
        return "(%f, %f)".formatted(this.latitude, this.longitude);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Coordinates that = (Coordinates) o;
        return Objects.equals(latitude, that.latitude) && Objects.equals(longitude, that.longitude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latitude, longitude);
    }

}
