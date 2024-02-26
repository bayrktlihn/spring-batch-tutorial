package io.bayrktlihn.springbatchtutorial.dto;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@ToString
@XmlRootElement(name = "student")
public class StudentDto implements Serializable {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
}
