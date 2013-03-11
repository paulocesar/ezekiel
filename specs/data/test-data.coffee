makeFighter = (firstName, lastName, dob, country, heightInCm, reachInCm, weightInLb) -> {
    firstName, lastName, dOB: dob, country, heightInCm, reachInCm, weightInLb
}
data = {
    fighters: [
        makeFighter('Anderson', 'Silva', '1975-04-14', 'Brazil', 188, 197, 185)
        makeFighter('Wanderlei', 'Silva', '1976-07-02', 'Brazil', 180, 188, 204)
        makeFighter('Jon', 'Jones', '1987-07-19', 'USA', 193, 215, 205)
        makeFighter('Cain', 'Velasquez', '1982-07-28', 'USA', 185, 196, 240)
    ]

    makeFighter

    # I keep telling my brother to drop out of residency and start his MMA carreer
    # before it's too late
    newFighter: () -> makeFighter('Guilherme', 'Duarte', '1987-03-14', 'Brazil', 180, 188, 175)
}

data.cntFighters = data.fighters.length

module.exports = data
